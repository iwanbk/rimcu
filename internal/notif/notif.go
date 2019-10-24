package notif

import (
	"github.com/shamaton/msgpack"
)

type Notif struct {
	ClientID []byte
	Key      string
	Slot     uint64
}

func (n *Notif) Encode() ([]byte, error) {
	return msgpack.Encode(n)
}

func Decode(b []byte) (*Notif, error) {
	var n Notif
	return &n, msgpack.Decode(b, &n)
}
