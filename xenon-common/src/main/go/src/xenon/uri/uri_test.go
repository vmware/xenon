package uri

import (
	"encoding/json"
	"testing"
)

func TestMarshalOnValue(t *testing.T) {
	var u1, u2 URI

	u1 = New("localhost", "80")
	b, _ := json.Marshal(u1)
	json.Unmarshal(b, &u2)

	if u1.Host != u2.Host {
		t.Errorf("Encoding/decoding error")
	}
}

func TestExtendQuery(t *testing.T) {
	u := New("localhost", "80")
	u = ExtendQuery(u, "foo", "bar")

	expect := "http://localhost:80/?foo=bar"
	if u.String() != expect {
		t.Errorf("expected '%s', got '%s'", expect, u)
	}
}
