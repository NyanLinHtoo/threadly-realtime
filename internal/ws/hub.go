package ws

import "log"

type Hub struct {
	clients    map[string]map[*Client]bool // conversationID -> set of clients
	register   chan *Client
	unregister chan *Client
	broadcast  chan BroadcastMessage
}

func (h *Hub) Register(client *Client) {
	h.register <- client
}

func (h *Hub) Unregister(client *Client) {
	h.unregister <- client
}

func (h *Hub) Broadcast(message BroadcastMessage) {
	h.broadcast <- message
}

type BroadcastMessage struct {
	ConversationID string
	Payload        []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan BroadcastMessage),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			if h.clients[client.ConversationID] == nil {
				h.clients[client.ConversationID] = make(map[*Client]bool)
			}
			h.clients[client.ConversationID][client] = true
			log.Printf("client registered: user=%s conversation=%s", client.UserID, client.ConversationID)

		case client := <-h.unregister:
			if clients, ok := h.clients[client.ConversationID]; ok {
				if _, ok := clients[client]; ok {
					delete(clients, client)
					close(client.Send)
					log.Printf("client unregistered: user=%s conversation=%s", client.UserID, client.ConversationID)
				}
			}

		case message := <-h.broadcast:
			for client := range h.clients[message.ConversationID] {
				select {
				case client.Send <- message.Payload:
				default:
					close(client.Send)
					delete(h.clients[message.ConversationID], client)
				}
			}
		}
	}
}
