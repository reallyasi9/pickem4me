package pickem4me

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
	"gonum.org/v1/gonum/stat/distuv"
)

// projectID is supplied by the environment, set automatically in GCF
var projectID = os.Getenv("GCP_PROJECT")

// fsclient is a lazily initialized firestore client
var fsclient *firestore.Client

// csclient is a Cloud Store client
var csclient *storage.Client

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// PickEmMessage tells the function what to pick and for whom.
type PickEmMessage struct {
	// Slate is the path to a parsed slate in Firestore
	Slate string `json:"slate"`

	// Picker is the path to a picker in Firestore
	Picker string `json:"picker"`

	// Model is a path to a model to use when picking (empty value means use the best model possible)
	Model string `json:"model"`
}

// Team is a simple representation of a team in Firestore.
type Team struct {
	School string `firestore:"school"`
	Name   string `firestore:"team"`
}

// ModelPerformance prepresents the performacne of a particular model in Firestore.
type ModelPerformance struct {
	Model  *firestore.DocumentRef `firestore:"model"`
	StdDev float64                `firestore:"std_dev"`
	Bias   float64                `firestore:"bias"`
	System string                 `firestore:"system"`
}

// ModelPrediction represents a prediction for a particular game in Firestore.
type ModelPrediction struct {
	Home    *firestore.DocumentRef `firestore:"home"`
	Road    *firestore.DocumentRef `firestore:"road"`
	Neutral bool                   `firestore:"neutral"`
	Spread  float64                `firestore:"spread"`
	// Ref is just a reference to the prediction itself
	Ref *firestore.DocumentRef
}

// Picker represents a picker in Firestore.
type Picker struct {
	Name     string `firestore:"name"`
	NameLuke string `firestore:"name_luke"`
}

// PickEm consumes a Pub/Sub message.
func PickEm(ctx context.Context, m PubSubMessage) error {
	var pem PickEmMessage
	err := json.Unmarshal(m.Data, &pem)
	if err != nil {
		log.Printf("json.Unmarshal: %v", err)
		return err
	}

	if fsclient == nil {
		fsclient, err = firestore.NewClient(ctx, projectID)
		if err != nil {
			log.Printf("Failed making Firestore client: %v", err)
			return err
		}
	}

	if csclient == nil {
		csclient, err = storage.NewClient(ctx)
		if err != nil {
			log.Printf("Failed making Cloud Storage client: %v", err)
			return err
		}
	}

	// Get the slate
	slateDoc, err := fsclient.Doc(pem.Slate).Get(ctx)
	if err != nil {
		log.Printf("Failed getting slate '%s': %v", pem.Slate, err)
		return err
	}
	var slate Slate
	err = slateDoc.DataTo(&slate)
	if err != nil {
		log.Printf("Failed parsing slate '%s': %v", pem.Slate, err)
		return err
	}
	log.Printf("Got slate '%s': %v", slateDoc.Ref.ID, slate)

	gameDocs, err := slateDoc.Ref.Collection("games").OrderBy("row", firestore.Asc).Documents(ctx).GetAll()
	if err != nil {
		log.Printf("Failed getting games from slate '%s': %v", pem.Slate, err)
		return err
	}
	games := make([]SlateGame, len(gameDocs))
	for i, doc := range gameDocs {
		var game SlateGame
		err = doc.DataTo(&game)
		if err != nil {
			log.Printf("Failed parsing game '%s': %v", doc.Ref.ID, err)
			return err
		}
		log.Printf("Got game '%s': %v", doc.Ref.ID, game)
		games[i] = game
	}

	// Get the picker
	pickerDoc, err := fsclient.Collection("pickers").Where("name_luke", "==", pem.Picker).Limit(1).Documents(ctx).Next()
	if err != nil {
		log.Printf("Failed getting picker '%s': %v", pem.Picker, err)
		return err
	}
	var picker Picker
	if err := pickerDoc.DataTo(&picker); err != nil {
		log.Printf("Failed parsing picker '%s': %v", pem.Picker, err)
		return err
	}
	log.Printf("Got picker '%s': %v", pickerDoc.Ref.ID, picker)

	// Figure out the model to use
	modelPerfDoc, err := GetModel(ctx, pem.Model)
	if err != nil {
		log.Printf("Failed getting model '%s': %v", pem.Model, err)
		return err
	}
	var modelPerf ModelPerformance
	if err := modelPerfDoc.DataTo(&modelPerf); err != nil {
		log.Printf("Failed parsing model performance '%s': %v", pem.Model, err)
		return err
	}
	log.Printf("Got model performance for '%s': %v", modelPerfDoc.Ref.ID, modelPerf)

	predictionDocs, err := modelPerfDoc.Ref.Collection("predictions").Documents(ctx).GetAll()
	if err != nil {
		log.Printf("Failed getting predictions from model '%s': %v", pem.Model, err)
		return err
	}
	predictions := make([]ModelPrediction, len(predictionDocs))
	for i, doc := range predictionDocs {
		var prediction ModelPrediction
		if err := doc.DataTo(&prediction); err != nil {
			log.Printf("Failed parsing prediction '%s': %v", doc.Ref.ID, err)
			return err
		}
		log.Printf("Got prediction '%s': %v", doc.Ref.ID, prediction)
		prediction.Ref = doc.Ref
		predictions[i] = prediction
	}

	// Convert performance model into a probability distribution
	modelDist := distuv.Normal{
		Mu:    modelPerf.Bias,
		Sigma: modelPerf.StdDev,
	}

	// Start calculating picks and probabilities
	// Make a lookup table for home and road teams
	homeLookup := make(map[string]*ModelPrediction)
	roadLookup := make(map[string]*ModelPrediction)
	for i := range predictions {
		model := predictions[i]
		homeLookup[model.Home.ID] = &predictions[i]
		roadLookup[model.Road.ID] = &predictions[i]
	}

	// closure to lookup home and road teams
	lookup := func(home, road *firestore.DocumentRef) (*ModelPrediction, bool, error) {
		var hr, rr *ModelPrediction
		var ok, maybeSwap bool

		hr, ok = homeLookup[home.ID]
		// if found, everything is fine.
		// if not, maybe Luke got home and road mixed up?
		if !ok {
			hr, maybeSwap = roadLookup[home.ID]
			if !maybeSwap {
				return nil, false, fmt.Errorf("home team '%s' not in predicted game", home.ID)
			}
		}

		// if home is where it should be, road should be found as well
		if !maybeSwap {
			rr, ok = roadLookup[road.ID]
			if !ok {
				return nil, false, fmt.Errorf("road team '%s' not in predicted game", road.ID)
			}
		} else {
			// road should be in home list
			rr, ok = homeLookup[road.ID]
			if !ok {
				return nil, false, fmt.Errorf("road->home team '%s' not in predicted game", road.ID)
			}
		}

		// now check if the predicted games are the same (i.e., home is actually playing road!)
		if hr != rr {
			return nil, false, fmt.Errorf("home '%s' and road '%s' not playing each other", home.ID, road.ID)
		}

		log.Printf("")

		return hr, maybeSwap, nil
	}

	// Pick-a-dog
	var pickedDog *SuperDogPick
	var bestValue float64

	// Make picks separate from slate games
	suPicks := make([]*StraightUpPick, 0)
	nsPicks := make([]*NoisySpreadPick, 0)
	sdPicks := make([]*SuperDogPick, 0)

	for _, game := range games {
		modelPred, swap, err := lookup(game.Home, game.Road)
		if err != nil {
			log.Printf("Failed looking up prediction: %v", err)
			return err
		}
		log.Printf("Found prediction for teams %s and %s: %v", game.Home.ID, game.Road.ID, *modelPred)

		// Remember that positive spread always favors home in the predictions.
		// It also needs to favor the overdog for calculating probability correctly.
		spread := modelPred.Spread
		if game.Superdog {
			if modelPred.Road.ID == game.Underdog.ID {
				spread *= -1
			}
		}

		target := game.NoisySpread
		if swap {
			target *= -1
		}
		prob := modelDist.CDF(spread - float64(target))

		// Disagreement over neutral site?
		neutralDisagreement := game.NeutralSite != modelPred.Neutral

		// Update game
		if game.Superdog {
			sdp := SuperDogPick{
				Underdog:             game.Underdog,
				Overdog:              game.Overdog,
				Rank1:                game.Rank1,
				Rank2:                game.Rank2,
				Value:                game.Value,
				NeutralSite:          game.NeutralSite != neutralDisagreement, // logic: 0,0->0, 0,1->1, 1,0->1, 1,1->0
				NeutralDisagreement:  neutralDisagreement,
				Swap:                 swap,
				Pick:                 nil, // hold of on picking superdogs
				PredictedSpread:      modelPred.Spread,
				PredictedProbability: prob,
				ModeledGame:          modelPred.Ref,
				Row:                  game.Row,
			}
			sdPicks = append(sdPicks, &sdp)
			ev := prob * float64(game.Value)
			if ev > bestValue {
				pickedDog = &sdp
				bestValue = ev
			}
			continue
		}

		pick := modelPred.Home
		if prob < 0.5 {
			pick = modelPred.Road
		}
		if game.NoisySpread > 0 {
			nsPicks = append(nsPicks, &NoisySpreadPick{
				Home:                 modelPred.Home,
				Road:                 modelPred.Road,
				Rank1:                game.Rank1,
				Rank2:                game.Rank2,
				NoisySpread:          game.NoisySpread,
				NeutralSite:          game.NeutralSite != neutralDisagreement, // logic: 0,0->0, 0,1->1, 1,0->1, 1,1->0
				NeutralDisagreement:  neutralDisagreement,
				Swap:                 swap,
				Pick:                 pick,
				PredictedSpread:      modelPred.Spread,
				PredictedProbability: prob,
				ModeledGame:          modelPred.Ref,
				Row:                  game.Row,
			})
			continue
		}
		suPicks = append(suPicks, &StraightUpPick{
			Home:                 modelPred.Home,
			Road:                 modelPred.Road,
			Rank1:                game.Rank1,
			Rank2:                game.Rank2,
			GOTW:                 game.GOTW,
			NeutralSite:          game.NeutralSite != neutralDisagreement, // logic: 0,0->0, 0,1->1, 1,0->1, 1,1->0
			NeutralDisagreement:  neutralDisagreement,
			Swap:                 swap,
			Pick:                 pick,
			PredictedSpread:      modelPred.Spread,
			PredictedProbability: prob,
			ModeledGame:          modelPred.Ref,
			Row:                  game.Row,
		})
	}

	// Pick that dog!
	pickedDog.Pick = pickedDog.Underdog

	// With picks in place, write to Firestore
	picksColl := fsclient.Collection("picks")
	picksRef := picksColl.NewDoc()
	_, err = picksRef.Create(ctx, &Picks{
		Season: slate.Season,
		Week:   slate.Week,
		Picker: pickerDoc.Ref,
	})
	if err != nil {
		return fmt.Errorf("Failed to write picks to firestore: %v", err)
	}

	suColl := picksRef.Collection("straight_up")
	nsColl := picksRef.Collection("noisy_spread")
	sdColl := picksRef.Collection("superdog")
	fsclient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		for _, pick := range suPicks {
			ref := suColl.NewDoc()
			if err := tx.Create(ref, pick); err != nil {
				return fmt.Errorf("Transaction failed to create StraightUpPick: %v", err)
			}
		}
		for _, pick := range nsPicks {
			ref := nsColl.NewDoc()
			if err := tx.Create(ref, pick); err != nil {
				return fmt.Errorf("Transaction failed to create NoisySpreadPick: %v", err)
			}
		}
		for _, pick := range sdPicks {
			ref := sdColl.NewDoc()
			if err := tx.Create(ref, pick); err != nil {
				return fmt.Errorf("Transaction failed to create SuperDogPick: %v", err)
			}
		}
		return nil
	})
	bucket := csclient.Bucket(slate.BucketName)
	obj := bucket.Object("picks/" + slate.File)
	w := obj.NewWriter(ctx)
	defer w.Close()

	outExcel, err := newExcelFile(ctx, suPicks, nsPicks, sdPicks)
	if err != nil {
		return err
	}

	outExcel.Write(w)

	return nil
}

// GetModel returns the model requested by the given identifier string, or the most conservative model if an empty path is given.
func GetModel(ctx context.Context, path string) (*firestore.DocumentSnapshot, error) {

	latestTracker, err := fsclient.Collection("prediction_tracker").OrderBy("timestamp", firestore.Desc).Limit(1).Documents(ctx).Next()
	if err != nil {
		return nil, fmt.Errorf("Failed to get latest prediction tracker: %v", err)
	}

	modelPerfs := latestTracker.Ref.Collection("model_performance")

	if path == "" {
		log.Printf("No model requested: calculating best model at the time of pick")

		// Most conservative model (by MAE)
		bestModel, err := modelPerfs.OrderBy("mae", firestore.Desc).Limit(1).Documents(ctx).Next()
		if err != nil {
			return nil, fmt.Errorf("Failed to get best model: %v", err)
		}

		return bestModel, nil
	}

	modelRef := fsclient.Doc(path)
	model, err := modelPerfs.Where("model", "==", modelRef).Limit(1).Documents(ctx).Next()
	if err != nil {
		return nil, fmt.Errorf("Failed to get model at path '%s': %v", path, err)
	}

	return model, nil
}
