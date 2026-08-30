package ws

import "log"

type NotificationHub struct {
	clients    map[string]map[*Client]bool // userId -> set of clients
	register   chan *Client
	unregister chan *Client
	notify     chan NotificationMessage
}

type NotificationMessage struct {
	UserID  string
	Payload []byte
}

func NewNotificationHub() *NotificationHub {
	return &NotificationHub{
		clients:    make(map[string]map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		notify:     make(chan NotificationMessage),
	}
}

func (h *NotificationHub) Run() {
	for {
		select {
		case client := <-h.register:
			if h.clients[client.UserID] == nil {
				h.clients[client.UserID] = make(map[*Client]bool)
			}
			h.clients[client.UserID][client] = true
			log.Printf("notification client registered: user=%s", client.UserID)

		case client := <-h.unregister:
			if clients, ok := h.clients[client.UserID]; ok {
				if _, ok := clients[client]; ok {
					delete(clients, client)
					close(client.Send)
					log.Printf("notification client registered: user=%s", client.UserID)
				}
			}

		case message := <-h.notify:
			for client := range h.clients[message.UserID] {
				select {
				case client.Send <- message.Payload:
				default:
					close(client.Send)
					delete(h.clients[message.UserID], client)
				}
			}
		}
	}
}

func (h *NotificationHub) Register(client *Client)        { h.register <- client }
func (h *NotificationHub) Unregister(client *Client)      { h.unregister <- client }
func (h *NotificationHub) Notify(msg NotificationMessage) { h.notify <- msg }
