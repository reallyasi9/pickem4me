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
		Model:  "models/linesag",
		Slate:  "slates/jkzMZ-ixAFyUaO1aDJC6GBXM5i7T2nvLZVoXaBytLas",
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
