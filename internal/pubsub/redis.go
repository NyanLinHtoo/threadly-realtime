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
	Event          json.RawMessage `json:"event"`
}

type UserNotification struct {
	UserID  string          `json:"userId"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
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
			Payload:        incoming.Event,
		})
	}
}

func SubscribeAndNotify(client *redis.Client, hub *ws.NotificationHub) {
	ctx := context.Background()
	sub := client.Subscribe(ctx, "user_notifications")

	for msg := range sub.Channel() {
		var notification UserNotification
		if err := json.Unmarshal([]byte(msg.Payload), &notification); err != nil {
			log.Println("failed to parse notification:", err)
			continue
		}

		hub.Notify(ws.NotificationMessage{
			UserID:  notification.UserID,
			Payload: []byte(msg.Payload),
		})
	}
}
