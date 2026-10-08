package queue

import (
	"bytes"
	"testing"
	"time"
)

func TestEncodeDecode(t *testing.T) {
	at := time.Unix(1_700_000_000, 123456789)
	a, b := Encode(7, at, 256), Encode(8, at, 256)
	if len(a) != 256 {
		t.Fatalf("len=%d", len(a))
	}
	seq, scheduled, err := Decode(a)
	if err != nil || seq != 7 || !scheduled.Equal(at) {
		t.Fatalf("decode: %d %v %v", seq, scheduled, err)
	}
	if bytes.Equal(a[HeaderSize:], b[HeaderSize:]) {
		t.Fatal("filler repeats between messages")
	}
	if len(Encode(1, at, 4)) != HeaderSize {
		t.Fatal("payload smaller than the header")
	}
	if _, _, err := Decode(a[:10]); err == nil {
		t.Fatal("short payload accepted")
	}
}
