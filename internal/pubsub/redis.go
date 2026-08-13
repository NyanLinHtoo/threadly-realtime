package pubsub

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"

	"chat-app-realtime/internal/ws"
)

type IncomingMessage struct {
	ConversationID string          `json:"conversationId"`
	Message        json.RawMessage `json:"message"`
}

func SubscribeAndBroadcast(client *redis.Client, hub *ws.Hub) {
	ctx := context.Background()
	sub := client.Subscribe(ctx, "new_message")

	for msg := range sub.Channel() {
		var incoming IncomingMessage
		if err := json.Unmarshal([]byte(msg.Payload), &incoming); err != nil {
			log.Println("failed to parse redis message:", err)
			continue
		}

		hub.Broadcast(ws.BroadcastMessage{
			ConversationID: incoming.ConversationID,
			Payload:        incoming.Message,
		})
	}
}
