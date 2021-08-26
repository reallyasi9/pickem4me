package pickem4me

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
)

// SlatePrinter makes a set of strings to print to a slate spreadsheet.
type SlatePrinter interface {
	// SlateRow creates a row of strings for direct output to a slate spreadsheet.
	SlateRow(context.Context) ([]string, error)
}

// Slate represents a slate in Firestore.
type Slate struct {
	BucketName string                 `firestore:"bucket_name"`
	Created    time.Time              `firestore:"created"`
	File       string                 `firestore:"file"`
	Season     *firestore.DocumentRef `firestore:"season"`
	Week       int                    `firestore:"week"`
}

// SlateGame represents a game in Firestore as parsed from a slate.
type SlateGame struct {
	GOTW        bool                   `firestore:"gotw"`
	Home        *firestore.DocumentRef `firestore:"home"`
	NeutralSite bool                   `firestore:"neutral_site"`
	NoisySpread int                    `firestore:"noisy_spread"`
	Overdog     *firestore.DocumentRef `firestore:"overdog"`
	Rank1       int                    `firestore:"rank1"`
	Rank2       int                    `firestore:"rank2"`
	Road        *firestore.DocumentRef `firestore:"road"`
	Superdog    bool                   `firestore:"superdog"`
	Underdog    *firestore.DocumentRef `firestore:"underdog"`
	Value       int                    `firestore:"value"`
	Row         int                    `firestore:"row"`
}
