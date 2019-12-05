package pickem4me

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestPickEm(t *testing.T) {
	ctx := context.Background()
	pem := PickEmMessage{
		Picker: os.Getenv("picker"),
		// Model:  "models/line",
		Slate: "slates/" + os.Getenv("slate"),
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
