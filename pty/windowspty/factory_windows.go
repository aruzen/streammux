//go:build windows

// Package windowspty implements pty.Factory and pty.ManagedFactory with
// Windows ConPTY and Job Objects.
package windowspty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/aruzen/streammux/pty"
	"golang.org/x/sys/windows"
)

var (
	// ErrSizeOverflow indicates a terminal size unsupported by ConPTY.
	ErrSizeOverflow = errors.New("windowspty: terminal size exceeds int16")
	// ErrInvalidProcessSpec indicates a NUL-containing exec parameter.
	ErrInvalidProcessSpec = errors.New("windowspty: process specification contains NUL")
)

var releasePseudoConsole = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReleasePseudoConsole")

const (
	defaultColumns             = 80
	defaultRows                = 24
	jobExitCode                = 1
	procThreadAttributeJobList = 0x0002000d
)

// Factory implements the tagged v0.1 pty.Factory API. The process lifetime is
// tied to ctx. Close terminates the process tree and releases all handles.
type Factory struct{}

// Start starts a legacy Process. A zero InitialSize uses 80x24 for compatibility
// with the legacy API, which did not require an initial terminal size.
func (Factory) Start(ctx context.Context, spec pty.ProcessSpec) (pty.Process, error) {
	if spec.InitialSize == (pty.Size{}) {
		spec.InitialSize = pty.Size{Cols: defaultColumns, Rows: defaultRows}
	}
	managed, err := startManaged(ctx, spec)
	if err != nil {
		return nil, err
	}
	legacy := &process{managed: managed}
	go func() {
		select {
		case <-ctx.Done():
			_ = managed.Kill()
		case <-managed.done:
		}
	}()
	return legacy, nil
}

type process struct {
	managed *managedProcess
}

func (p *process) Output() io.Reader          { return p.managed.Output() }
func (p *process) Input() io.Writer           { return p.managed.Input() }
func (p *process) Resize(size pty.Size) error { return p.managed.Resize(size) }
func (p *process) Kill() error                { return p.managed.Kill() }
func (p *process) ExitCode() int              { status, _ := p.managed.WaitStatus(); return status.Code }
func (p *process) Wait() error                { return legacyWait(p.managed) }
func (p *process) Close() error {
	closeErr := p.managed.Close()
	killErr := p.managed.Kill()
	_, waitErr := p.managed.WaitStatus()
	return errors.Join(closeErr, killErr, waitErr)
}

func legacyWait(process *managedProcess) error {
	status, err := process.WaitStatus()
	if err != nil {
		return err
	}
	if status.Code != 0 {
		return fmt.Errorf("windowspty: process exited with code %d", status.Code)
	}
	return nil
}

// ManagedFactory implements pty.ManagedFactory with ConPTY. Windows 10 version
// 1809 or newer is required. The returned process owns a Job Object containing
// the complete process tree.
type ManagedFactory struct{}

// StartManaged starts a ConPTY client. ctx controls startup only; after this
// method succeeds the caller owns the process and must terminate or wait for it,
// then call Close. Output must be drained while the process runs.
func (ManagedFactory) StartManaged(ctx context.Context, spec pty.ProcessSpec) (pty.ManagedProcess, error) {
	return startManaged(ctx, spec)
}

func startManaged(ctx context.Context, spec pty.ProcessSpec) (*managedProcess, error) {
	if ctx == nil {
		return nil, errors.New("windowspty: nil context")
	}
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	executable, err := exec.LookPath(spec.Command)
	if err != nil {
		return nil, fmt.Errorf("windowspty: resolve command: %w", err)
	}
	application, err := windows.UTF16PtrFromString(executable)
	if err != nil {
		return nil, ErrInvalidProcessSpec
	}
	commandLine, err := windows.UTF16FromString(windows.ComposeCommandLine(append([]string{executable}, spec.Args...)))
	if err != nil {
		return nil, ErrInvalidProcessSpec
	}
	environment, err := environmentBlock(spec.Env)
	if err != nil {
		return nil, err
	}
	var environmentPointer *uint16
	if environment != nil {
		environmentPointer = &environment[0]
	}
	var directory *uint16
	if spec.Dir != "" {
		directory, err = windows.UTF16PtrFromString(spec.Dir)
		if err != nil {
			return nil, ErrInvalidProcessSpec
		}
	}

	var inputRead, inputWrite, outputRead, outputWrite windows.Handle
	closeHandle := func(handle *windows.Handle) {
		if *handle != 0 && *handle != windows.InvalidHandle {
			_ = windows.CloseHandle(*handle)
			*handle = 0
		}
	}
	if err = windows.CreatePipe(&inputRead, &inputWrite, nil, 0); err != nil {
		return nil, fmt.Errorf("windowspty: create input pipe: %w", err)
	}
	defer closeHandle(&inputRead)
	defer closeHandle(&inputWrite)
	if err = windows.CreatePipe(&outputRead, &outputWrite, nil, 0); err != nil {
		return nil, fmt.Errorf("windowspty: create output pipe: %w", err)
	}
	defer closeHandle(&outputRead)
	defer closeHandle(&outputWrite)

	var console windows.Handle
	size := windows.Coord{X: int16(spec.InitialSize.Cols), Y: int16(spec.InitialSize.Rows)}
	if err = windows.CreatePseudoConsole(size, inputRead, outputWrite, 0, &console); err != nil {
		return nil, fmt.Errorf("windowspty: create pseudo console: %w", err)
	}
	consoleOwned := true
	defer func() {
		if consoleOwned {
			windows.ClosePseudoConsole(console)
		}
	}()

	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("windowspty: create job object: %w", err)
	}
	jobOwned := true
	defer func() {
		if jobOwned {
			_ = windows.CloseHandle(job)
		}
	}()
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		return nil, fmt.Errorf("windowspty: configure job object: %w", err)
	}
	attributes, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return nil, fmt.Errorf("windowspty: allocate process attributes: %w", err)
	}
	defer attributes.Delete()
	if err = attributes.Update(windows.PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, pseudoConsoleAttributeValue(console), unsafe.Sizeof(console)); err != nil {
		return nil, fmt.Errorf("windowspty: set pseudo console attribute: %w", err)
	}
	jobs := []windows.Handle{job}
	if err = attributes.Update(procThreadAttributeJobList, unsafe.Pointer(&jobs[0]), unsafe.Sizeof(jobs[0])); err != nil {
		return nil, fmt.Errorf("windowspty: set job list attribute: %w", err)
	}
	startup := windows.StartupInfoEx{
		StartupInfo: windows.StartupInfo{
			Cb:    uint32(unsafe.Sizeof(windows.StartupInfoEx{})),
			Flags: windows.STARTF_USESTDHANDLES,
		},
		ProcThreadAttributeList: attributes.List(),
	}

	information := windows.ProcessInformation{}
	flags := uint32(windows.CREATE_UNICODE_ENVIRONMENT | windows.EXTENDED_STARTUPINFO_PRESENT)
	if err = windows.CreateProcess(application, &commandLine[0], nil, nil, false, flags, environmentPointer, directory, &startup.StartupInfo, &information); err != nil {
		return nil, fmt.Errorf("windowspty: create process: %w", err)
	}
	processOwned, threadOwned := true, true
	defer func() {
		if threadOwned {
			_ = windows.CloseHandle(information.Thread)
		}
		if processOwned {
			_ = windows.TerminateProcess(information.Process, jobExitCode)
			_, _ = windows.WaitForSingleObject(information.Process, windows.INFINITE)
			_ = windows.CloseHandle(information.Process)
		}
	}()
	_ = windows.CloseHandle(information.Thread)
	threadOwned = false
	// CreatePseudoConsole duplicates these ends into ConHost. Keeping them in
	// the host prevents EOF and can deadlock pseudoconsole shutdown.
	closeHandle(&inputRead)
	closeHandle(&outputWrite)

	inputFile := os.NewFile(uintptr(inputWrite), "conpty-input")
	outputFile := os.NewFile(uintptr(outputRead), "conpty-output")
	if inputFile == nil || outputFile == nil {
		if inputFile != nil {
			_ = inputFile.Close()
			inputWrite = 0
		}
		if outputFile != nil {
			_ = outputFile.Close()
			outputRead = 0
		}
		return nil, errors.New("windowspty: wrap pipe handles")
	}
	inputWrite, outputRead = 0, 0

	result := &managedProcess{
		process: information.Process,
		job:     job,
		console: console,
		input:   inputFile,
		output:  &output{file: outputFile, observed: make(chan struct{})},
		done:    make(chan struct{}),
		running: true,
	}
	if releasePseudoConsole.Find() == nil {
		_, _, _ = releasePseudoConsole.Call(uintptr(console))
		result.released = true
	}
	consoleOwned, jobOwned, processOwned = false, false, false

	select {
	case <-ctx.Done():
		_ = result.Close()
		_ = result.Kill()
		_, _ = result.WaitStatus()
		return nil, ctx.Err()
	default:
		return result, nil
	}
}

// HPCON is declared as a pointer by Win32 but represented as uintptr by
// x/sys/windows. UpdateProcThreadAttribute expects that pointer value itself,
// not the address of the Go Handle variable.
func pseudoConsoleAttributeValue(console windows.Handle) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&console))
}

func validateSpec(spec pty.ProcessSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	if err := spec.InitialSize.Validate(); err != nil {
		return err
	}
	if spec.InitialSize.Cols > math.MaxInt16 || spec.InitialSize.Rows > math.MaxInt16 {
		return ErrSizeOverflow
	}
	values := append([]string{spec.Command, spec.Dir}, spec.Args...)
	values = append(values, spec.Env...)
	for _, value := range values {
		if strings.IndexByte(value, 0) >= 0 {
			return ErrInvalidProcessSpec
		}
	}
	return nil
}

func environmentBlock(environment []string) ([]uint16, error) {
	if environment == nil {
		return nil, nil
	}
	values := append([]string(nil), environment...)
	sort.SliceStable(values, func(left, right int) bool {
		return strings.ToUpper(values[left]) < strings.ToUpper(values[right])
	})
	block := make([]uint16, 0)
	for _, value := range values {
		encoded, err := windows.UTF16FromString(value)
		if err != nil {
			return nil, ErrInvalidProcessSpec
		}
		block = append(block, encoded...)
	}
	block = append(block, 0)
	if len(block) == 1 {
		block = append(block, 0)
	}
	return block, nil
}

type output struct {
	file     *os.File
	observed chan struct{}
	once     sync.Once
}

func (o *output) Read(buffer []byte) (int, error) {
	n, err := o.file.Read(buffer)
	if n > 0 {
		o.once.Do(func() { close(o.observed) })
	}
	if errors.Is(err, windows.ERROR_BROKEN_PIPE) {
		return n, io.EOF
	}
	return n, err
}

type managedProcess struct {
	process     windows.Handle
	job         windows.Handle
	console     windows.Handle
	input       *os.File
	output      *output
	done        chan struct{}
	released    bool
	waitOnce    sync.Once
	waitStatus  pty.ExitStatus
	waitErr     error
	closeOnce   sync.Once
	closeErr    error
	consoleOnce sync.Once

	mu              sync.Mutex
	running         bool
	killed          bool
	terminateSent   bool
	consoleClosing  bool
	resourcesClosed bool
}

func (p *managedProcess) Output() io.Reader { return p.output }
func (p *managedProcess) Input() io.Writer  { return p.input }

func (p *managedProcess) Resize(size pty.Size) error {
	if err := size.Validate(); err != nil {
		return err
	}
	if size.Cols > math.MaxInt16 || size.Rows > math.MaxInt16 {
		return ErrSizeOverflow
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.running || p.consoleClosing {
		return nil
	}
	if err := windows.ResizePseudoConsole(p.console, windows.Coord{X: int16(size.Cols), Y: int16(size.Rows)}); err != nil {
		return fmt.Errorf("windowspty: resize pseudo console: %w", err)
	}
	return nil
}

// Terminate closes the pseudoconsole, which asks attached console processes to
// exit with CTRL_CLOSE_EVENT. Callers may follow it with Kill after a grace
// period to force the complete Job Object process tree to stop.
func (p *managedProcess) Terminate() error {
	p.mu.Lock()
	if p.resourcesClosed || p.consoleClosing || p.jobExitedLocked() {
		p.mu.Unlock()
		return nil
	}
	if p.running && !p.processExitedLocked() {
		p.terminateSent = true
	}
	p.mu.Unlock()
	p.closeConsole()
	return nil
}

// Kill terminates every process in the owned Job Object.
func (p *managedProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resourcesClosed || p.jobExitedLocked() {
		return nil
	}
	rootExited := p.processExitedLocked()
	if err := windows.TerminateJobObject(p.job, jobExitCode); err != nil {
		return fmt.Errorf("windowspty: terminate job object: %w", err)
	}
	if p.running && !rootExited {
		p.killed = true
	}
	return nil
}

// processExitedLocked closes the race between process completion and
// WaitStatus publishing running=false. The retained process handle also avoids
// PID-reuse ambiguity.
func (p *managedProcess) processExitedLocked() bool {
	event, err := windows.WaitForSingleObject(p.process, 0)
	return err == nil && event == windows.WAIT_OBJECT_0
}

func (p *managedProcess) jobExitedLocked() bool {
	event, err := windows.WaitForSingleObject(p.job, 0)
	return err == nil && event == windows.WAIT_OBJECT_0
}

func (p *managedProcess) WaitStatus() (pty.ExitStatus, error) {
	p.waitOnce.Do(func() {
		status := pty.ExitStatus{Reason: pty.ExitReasonExited, Code: -1}
		if event, err := windows.WaitForSingleObject(p.process, windows.INFINITE); err != nil || event != windows.WAIT_OBJECT_0 {
			if err == nil {
				err = fmt.Errorf("unexpected wait result %#x", event)
			}
			p.waitErr = fmt.Errorf("windowspty: wait process: %w", err)
			status.Reason = pty.ExitReasonIOFailure
			status.Error = p.waitErr.Error()
		} else {
			var code uint32
			if err = windows.GetExitCodeProcess(p.process, &code); err != nil {
				p.waitErr = fmt.Errorf("windowspty: get exit code: %w", err)
				status.Reason = pty.ExitReasonIOFailure
				status.Error = p.waitErr.Error()
			} else {
				status.Code = int(code)
			}
		}
		p.mu.Lock()
		p.running = false
		if p.killed {
			status.Reason = pty.ExitReasonKilled
		} else if p.terminateSent && status.Code != 0 && status.Reason == pty.ExitReasonExited {
			status.Reason = pty.ExitReasonSignaled
			status.Signal = "CTRL_CLOSE_EVENT"
		}
		p.waitStatus = status
		p.mu.Unlock()
		if !p.released {
			select {
			case <-p.output.observed:
			case <-time.After(100 * time.Millisecond):
			}
			p.closeConsole()
		}
		close(p.done)
	})
	<-p.done
	return p.waitStatus, p.waitErr
}

// Close releases host pipe handles. When the process is still running, the
// process and Job handles remain available to Kill and WaitStatus; final handle
// cleanup is deferred until WaitStatus completes.
func (p *managedProcess) Close() error {
	p.closeOnce.Do(func() {
		p.closeErr = errors.Join(normalizeClose(p.input.Close()), normalizeClose(p.output.file.Close()))
		p.mu.Lock()
		running := p.running
		p.mu.Unlock()
		if !running {
			p.closeResources()
			return
		}
		go func() {
			<-p.done
			p.closeResources()
		}()
	})
	return p.closeErr
}

func (p *managedProcess) closeConsole() {
	p.mu.Lock()
	p.consoleClosing = true
	p.mu.Unlock()
	p.consoleOnce.Do(func() { windows.ClosePseudoConsole(p.console) })
}

func (p *managedProcess) closeResources() {
	p.closeConsole()
	p.mu.Lock()
	if p.resourcesClosed {
		p.mu.Unlock()
		return
	}
	p.resourcesClosed = true
	process, job := p.process, p.job
	p.mu.Unlock()
	_ = windows.CloseHandle(process)
	_ = windows.CloseHandle(job)
}

func normalizeClose(err error) error {
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

var _ pty.Factory = Factory{}
var _ pty.Process = (*process)(nil)
var _ pty.ManagedFactory = ManagedFactory{}
var _ pty.ManagedProcess = (*managedProcess)(nil)
