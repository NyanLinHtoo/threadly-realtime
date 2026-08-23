# Threadly — Realtime

WebSocket service handling live message delivery for Threadly. Subscribes to Redis for new messages published by the [backend](../chat-app-backend) and broadcasts them to connected clients, scoped per conversation.

## Tech Stack

- **Language:** Go
- **WebSocket:** [gorilla/websocket](https://github.com/gorilla/websocket)
- **JWT:** [golang-jwt/jwt](https://github.com/golang-jwt/jwt) — verifies tokens issued by the Node backend (shared secret)
- **Pub/Sub:** [go-redis](https://github.com/redis/go-redis)
- **Env loading:** [godotenv](https://github.com/joho/godotenv)

## Architecture

One WebSocket connection = one conversation (not one persistent connection for the whole session). The client closes and reopens a connection when switching conversations. A concurrency-safe `Hub` (goroutine + channels) owns all connection state; nothing outside `Hub.Run()`'s loop ever touches the connections map directly.

```
Node backend --(publish)--> Redis --(subscribe)--> Go Hub --(broadcast)--> WebSocket clients in that conversation's room
```

### Auth: the ws-token pattern

Because the frontend, backend, and this service are deployed on separate domains, the backend's httpOnly auth cookie isn't visible here. Instead: the frontend calls `GET /auth/ws-token` on the backend (same-domain, cookie works fine there) to get a short-lived copy of the JWT, then passes it explicitly as a query param on the WebSocket connection: `?conversationId=...&token=...`.

## Setup

1. Install dependencies:
   ```bash
   go mod download
   ```

2. Create a `.env` file:
   ```env
   JWT_SECRET=...          # MUST match the backend's JWT_SECRET exactly
   REDIS_ADDR=localhost:6379
   FRONTEND_URL=http://localhost:3000
   PORT=8080
   ```

3. Run:
   ```bash
   go run ./cmd/server
   ```

## Project Structure

```
cmd/server/main.go          # entry point — wiring only
internal/
  auth/jwt.go                 # JWT verification (shared secret with the Node backend)
  ws/
    client.go                  # per-connection read/write goroutines
    hub.go                      # owns all connection state; only touched from its own goroutine
  pubsub/redis.go             # Redis subscription -> Hub.Broadcast bridge
```

## Endpoints

| Route | Description |
|---|---|
| `GET /health` | Health check |
| `GET /ws?conversationId=&token=` | WebSocket upgrade — verifies the token, joins the conversation's room |

## Deployment

Deployed on [Render](https://render.com) (Docker). Set the same env vars as above, with `FRONTEND_URL` pointing to the real deployed frontend. `REDIS_ADDR`/Redis auth should point to your managed Redis instance (e.g. Upstash) with TLS enabled.