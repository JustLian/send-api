# aki-sender api

backend for a one-shot file sharing thing. two peers join a room, one pipes a file to the other through the server. nothing is stored.

## run it

```sh
go run ./cmd/server
```

listens on `:8080` by default. override with `ADDR=:1234`.

## layout

```
cmd/server/      entrypoint
internal/server/ http + ws + room/transfer state
```

## endpoints

- `GET  /ws`                                  — websocket control plane
- `GET  /download/{room}/{file}?sid=...`      — receiver pulls the file
- `POST /upload/{room}/{file}?sid=...`        — sender pushes the file
- `GET  /healthz`                             — `ok`

## notes

- one transfer per room, max 10 GiB
- cors is locked to `https://send.rian.moe` and `http://localhost:5173`. add more in `internal/server/server.go`.
- bytes are streamed through an `io.Pipe`, never touch disk.
