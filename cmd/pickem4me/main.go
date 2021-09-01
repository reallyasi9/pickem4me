package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/reallyasi9/pickem4me"
)

func usage() {
	w := flag.CommandLine.Output()
	fmt.Fprint(w, `pickem4me [flags] <picker> <slateID> [model]

Make all your picks for you!

Arguments:
	<picker>
		(Luke-given) name of picker.
	<slateID>
		The full Firebase path to the parsed slate.
	[model]
		The full Firebase path to the model (default: use the most conservative model for the type of pick)
Flags:
`)
	flag.PrintDefaults()
}

var _DRY_RUN bool

func init() {
	flag.BoolVar(&_DRY_RUN, "dryrun", false, "Do not write output to Firestore, just print the documents that would have been written.")
}

func main() {

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() < 2 || flag.NArg() > 3 {
		usage()
		os.Exit(0)
	}

	picker := flag.Arg(0)
	slateID := flag.Arg(1)
	var modelID string
	if flag.NArg() == 3 {
		modelID = flag.Arg(2)
	}

	ctx := context.Background()
	pem := pickem4me.PickEmMessage{
		Picker: picker,
		Model:  modelID,
		Slate:  slateID,
		DryRun: _DRY_RUN,
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
