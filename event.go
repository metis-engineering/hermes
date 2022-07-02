package hermes

import (
	"encoding/json"
)

type Event struct {
	ContextID string      `json:"context_id"`
	ServiceID string      `json:"service_id"`
	EventType EventType   `json:"event_type"`
	Body      interface{} `json:"body"`
}

func (ev *Event) Encode() ([]byte, error) {
	b, err := json.Marshal(ev)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func DecodeEvent(b []byte) *Event {
	ev := &Event{}
	_ = json.Unmarshal(b, ev)
	return ev
}

type EventType string

func (ev *EventType) MatchesAny(matches ...EventType) bool {
	for _, m := range matches {
		if *ev == m {
			return true
		}
	}
	return false
}
