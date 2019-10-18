package pickem4me

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPickEm(t *testing.T) {
	ctx := context.Background()
	pem := PickEmMessage{
		Picker: "Phil K",
		// Model:  "models/line",
		Slate: "slates/LMcnOpUEn0r9xKfGfw5eK7DKRp7ffEQgDpjROsl5rws",
	}
	data, err := json.Marshal(pem)
	if err != nil {
		t.Fatal(err)
	}

	m := PubSubMessage{
		Data: data,
	}

	if err := PickEm(ctx, m); err != nil {
		t.Fatal(err)
	}
}
