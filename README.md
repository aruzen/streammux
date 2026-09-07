# streammux

`streammux` multiplexes opaque byte streams and typed request, response, and
event frames over one `io.ReadWriteCloser`. It also provides a semantic PTY
interface, a PTY-to-frame bridge, and a Linux/macOS PTY backend.

The framing core is independent of PTYs, JSON, operating systems, and
application message definitions.

```go
conn, err := streammux.Open(ctx, transport, streammux.DefaultConfig())
if err != nil {
    return err
}
defer conn.Close()

for {
    frame, err := conn.ReadFrame()
    if err != nil {
        return err
    }
    switch frame.Header.MessageType {
    case inputType:
        err = bridge.HandleInput(frame)
    case customControlType:
        err = handleControl(frame)
    }
    if err != nil {
        return err
    }
}
```

## Correlated protocols over one connection

`Peer` is the sole reader above a `Conn`. It provides message handler
registration, concurrent request correlation, per-StreamID ordered dispatch,
and bounded weighted outbound scheduling. Application and PTY message types can
therefore share one connection without an `Unhandled` catch-all.

```go
peer, err := streammux.NewPeer(conn, streammux.DefaultPeerConfig())
if err != nil {
    return err
}
if err := peer.Register(applicationType, applicationHandler); err != nil {
    return err
}
go func() { _ = peer.Serve(connectionContext) }()
```

Control, interactive, and bulk traffic default to weights 8:4:1. Queue limits
are enforced by both bytes and frame count. Ordering is retained within each
StreamID; different StreamIDs may dispatch concurrently. A handler must honor
its context. Received `Frame.Payload` may be retained but is immutable; copy it
before modification.

## Connection-independent PTY sessions

One daemon-lifetime `pty.Manager` owns all PTY processes. A connection-lifetime
`pty.Protocol` owns only that connection's attachments. Closing a Protocol or
Peer detaches its frontend; it does not terminate a Session. The same SessionID
can be attached through a later connection and replay bounded history before
live output.

```go
manager, err := pty.NewManager(daemonContext, unixpty.ManagedFactory{}, pty.DefaultManagerConfig())
if err != nil {
    return err
}
defer manager.Close() // terminates, waits, closes, and joins all owned sessions

protocol, err := pty.RegisterProtocol(peer, manager, pty.ProtocolConfig{
    Version: 1,
    Types: pty.MessageTypes{
        Open: 10, List: 11, Inspect: 12, Attach: 13, Detach: 14,
        Input: 15, Output: 16, Resize: 17, Kill: 18, Exit: 19,
        ReplayBegin: 20, ReplayEnd: 21, Error: 22,
    },
})
if err != nil {
    return err
}
defer protocol.Close()
```

`Manager.Open`'s context controls only startup. Once startup succeeds, the
Manager owns the process lifetime. `Session.Kill` starts cleanup that continues
even if the caller stops waiting. `OutputSink.Send` receives ordered
`AttachmentEvent` values: all remaining output followed by one terminal exit.
It must honor its context. Each attachment has an independent bounded queue and
bounded exit drain, so a slow or failed sink is detached without blocking the
PTY output pump or Session cleanup. Asynchronous attachment failures use the
configured `Error` event type on a best-effort basis before that Peer closes.

`DefaultManagerConfig` removes each Session after its final Output and Exit
events drain and its attachments stop. To permit reattach after process exit,
set `ManagerConfig.SessionLifecycle` to an explicit
`SessionLifecycleDescriptor` using `ExitedSessionRetain` and a positive
`MaxRetainedSessions`. Retained Sessions keep bounded history and the final
Exit event. `Manager.Remove` deletes one retained Session, while automatic FIFO
eviction enforces the descriptor limit. `MaxSessions` limits starting and
active processes independently of retained Sessions.

`MaxAttachmentsPerSession` is a positive, atomically enforced hard limit.
`SessionInfo` reports attachment count, final exit status, and history
availability. Manager observers receive `Attached`/`Detached` events with a
semantic close reason and `Retained`/`Evicted`/`Removed` lifecycle events.
`ManagerStats` reports total registered and active Session counts separately.

Output/history byte slices are immutable and may be retained by consumers.
They must not be modified. PTY Input and Output frame payloads are raw bytes;
the library performs no ANSI/VT parsing or transformation. `Peer.Stats` and
`Manager.Stats` expose cumulative traffic/lifecycle counters and per-stream
queue snapshots without exposing raw payloads.

For remote clients, use `pty.EncodeControl` with `pty.OpenRequest`,
`pty.ListRequest`, or `pty.AttachRequest`, and decode responses with
`pty.DecodeProtocolResponse`. Resize retains the fixed-width network-byte-order
encoding exposed by `pty.EncodeSize` and `pty.DecodeSize`.

Existing oct code can continue using the stable low-level `Conn`,
`pty.Factory`, `pty.Process`, and `pty.Bridge` APIs while migrating
independently.

Windows ConPTY/Job Object support and application capability negotiation are
outside the current release scope. Core and protocol packages still compile on
Windows; `pty/unixpty` targets Linux and macOS.

`Conn.Send` copies its payload. `Conn.SendOwned` transfers ownership and avoids
that copy; the caller must never mutate or reuse the payload after the call.

## Wire format

Each frame has a fixed 28-byte network-byte-order header followed by its raw
payload: payload length, version, message type, flags, correlation ID, and
stream ID. Applications provide optional validation for their own message
types and payload encoding.

## License

MIT
