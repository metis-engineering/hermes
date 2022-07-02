package hermes

var (
	serviceID = ""
)

func SetServiceID(id string) {
	serviceID = id
}

type Middleware func(ev *Event) *Event
