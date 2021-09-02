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
	fmt.Fprint(w, `pickem4me [flags] <picker> <slateID>

Make all your picks for you!

Arguments:
	<picker>
		(Luke-given) name of picker.
	<slateID>
		The full Firebase path to the parsed slate.
Flags:
`)
	flag.PrintDefaults()
}

var _DRY_RUN bool
var _SU_MODEL string
var _NS_MODEL string
var _SD_MODEL string

func init() {
	flag.BoolVar(&_DRY_RUN, "dryrun", false, "Do not write output to Firestore, just print the documents that would have been written.")

	flag.StringVar(&_SU_MODEL, "straightmodel", "", "The full Firebase path to a model to use for straight picks (default: use the model with the best win record this season.)")
	flag.StringVar(&_NS_MODEL, "noisyspreadmodel", "", "The full Firebase path to a model to use for noisy spread picks (default: use the model with the lowest mean absolute error this season.)")
	flag.StringVar(&_SD_MODEL, "superdogmodel", "", "The full Firebase path to a model to use for superdog picks (default: use model specified by `noisyspread`.)")
}

func main() {

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(0)
	}

	picker := flag.Arg(0)
	slateID := flag.Arg(1)

	ctx := context.Background()
	pem := pickem4me.PickEmMessage{
		Picker:           picker,
		StraightModel:    _SU_MODEL,
		NoisySpreadModel: _NS_MODEL,
		SuperdogModel:    _SD_MODEL,
		Slate:            slateID,
		DryRun:           _DRY_RUN,
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
