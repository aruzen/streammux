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

`Conn.Send` copies its payload. `Conn.SendOwned` transfers ownership and avoids
that copy; the caller must never mutate the payload after the call.

## Wire format

Each frame has a fixed 28-byte network-byte-order header followed by its raw
payload: payload length, version, message type, flags, correlation ID, and
stream ID. Applications provide optional validation for their own message
types and payload encoding.

## License

MIT
