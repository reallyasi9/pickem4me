package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/reallyasi9/pickem4me/internal/pickem4me"
)

const usage = `pickem4me <picker> <slateID>

Make all your picks for you!

Arguments
  - picker:  (Luke-given) name of picker.
  - slateID: The full Firebase path to the parsed slate.
  - model:   The full Firebase path to the model (default: use the best possible model for the type of pick)
`

func main() {

	if len(os.Args) < 3 {
		fmt.Println(usage)
		os.Exit(0)
	}

	picker := os.Args[1]
	slateID := os.Args[2]
	var modelID string
	if len(os.Args) > 3 {
		modelID = os.Args[3]
	}

	ctx := context.Background()
	pem := pickem4me.PickEmMessage{
		Picker: picker,
		Model:  modelID,
		Slate:  slateID,
	}

	data, err := json.Marshal(pem)
	if err != nil {
		log.Fatal(err)
	}

	m := pickem4me.PubSubMessage{
		Data: data,
	}

	if err := pickem4me.PickEm(ctx, m); err != nil {
		log.Fatal(err)
	}
}
