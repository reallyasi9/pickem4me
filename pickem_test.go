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
		Model:  "models/line",
		Slate:  "slates/IfO8E8jxew3MtuhnXyNfbeVxof7P2fcve398ct_rT2E",
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
