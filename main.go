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
	"google.golang.org/api/iterator"
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

// ModelPerformance prepresents the performance of a particular model in Firestore.
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

// StreakPrediction represents a simplified version of the Beat-the-Streak output in Firestore.
type StreakPrediction struct {
	BestPick    []*firestore.DocumentRef `firestore:"best_pick"`
	Probability float64                  `firestore:"probability"`
	Spread      float64                  `firestore:"spread"`
}

// Model is a collection of performance metrics, predictions, and a distribution.
type Model struct {
	Performance  ModelPerformance
	Predictions  []ModelPrediction
	Distribution distuv.Normal

	homeLookup map[string]int
	roadLookup map[string]int
}

// Lookup prediction by home and road teams, and whether or not to swap them for the slate.
func (m *Model) Lookup(home, road *firestore.DocumentRef) (*ModelPrediction, bool, error) {
	if m.homeLookup == nil || m.roadLookup == nil {
		// Make a lookup table for home and road teams
		m.homeLookup = make(map[string]int)
		m.roadLookup = make(map[string]int)
		for i, model := range m.Predictions {
			m.homeLookup[model.Home.ID] = i
			m.roadLookup[model.Road.ID] = i
		}
	}
	var hr, rr int
	var ok, maybeSwap bool

	hr, ok = m.homeLookup[home.ID]
	// if found, everything is fine.
	// if not, maybe Luke got home and road mixed up?
	if !ok {
		hr, maybeSwap = m.roadLookup[home.ID]
		if !maybeSwap {
			return nil, false, fmt.Errorf("home team '%s' not in predicted game", home.ID)
		}
	}

	// if home is where it should be, road should be found as well
	if !maybeSwap {
		rr, ok = m.roadLookup[road.ID]
		if !ok {
			return nil, false, fmt.Errorf("road team '%s' not in predicted game", road.ID)
		}
	} else {
		// road should be in home list
		rr, ok = m.homeLookup[road.ID]
		if !ok {
			return nil, false, fmt.Errorf("road->home team '%s' not in predicted game", road.ID)
		}
	}

	// now check if the predicted games are the same (i.e., home is actually playing road!)
	if hr != rr {
		return nil, false, fmt.Errorf("home '%s' and road '%s' not playing each other", home.ID, road.ID)
	}

	return &(m.Predictions[hr]), maybeSwap, nil
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
	modelPerfDocs, err := GetModels(ctx, pem.Model)
	if err != nil {
		log.Printf("Failed getting model '%s': %v", pem.Model, err)
		return err
	}

	models := make(map[string]Model)
	for gameType, doc := range modelPerfDocs {
		var modelPerf ModelPerformance
		if err := doc.DataTo(&modelPerf); err != nil {
			log.Printf("Failed parsing model performance '%s': %v", pem.Model, err)
			return err
		}
		log.Printf("Got model performance for '%s': %v", doc.Ref.ID, modelPerf)

		predictionDocs, err := doc.Ref.Collection("predictions").Documents(ctx).GetAll()
		if err != nil {
			log.Printf("Failed getting predictions from model '%s': %v", pem.Model, err)
			return err
		}
		preds := make([]ModelPrediction, len(predictionDocs))
		for i, doc := range predictionDocs {
			var prediction ModelPrediction
			if err := doc.DataTo(&prediction); err != nil {
				log.Printf("Failed parsing prediction '%s': %v", doc.Ref.ID, err)
				return err
			}
			log.Printf("Got prediction '%s': %v", doc.Ref.ID, prediction)
			prediction.Ref = doc.Ref
			preds[i] = prediction
		}

		dist := distuv.Normal{
			Mu:    modelPerf.Bias,
			Sigma: modelPerf.StdDev,
		}

		models[gameType] = Model{Predictions: preds, Performance: modelPerf, Distribution: dist}
	}

	// Pick-a-dog
	var pickedDog *SuperDogPick
	var bestValue float64

	// Make picks separate from slate games
	suPicks := make([]*StraightUpPick, 0)
	nsPicks := make([]*NoisySpreadPick, 0)
	sdPicks := make([]*SuperDogPick, 0)

	for _, game := range games {
		gameType := "StraightUp"
		if game.Superdog {
			gameType = "Superdog"
		}
		if game.NoisySpread != 0 {
			gameType = "NoisySpread"
		}

		model := models[gameType]
		modelPred, swap, err := model.Lookup(game.Home, game.Road)
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
		prob := model.Distribution.CDF(spread - float64(target))

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
		if game.NoisySpread != 0 {
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

	// Finally look up streak
	streakPick, err := LookupStreakPick(ctx, pickerDoc.Ref, slate.Season, slate.Week)
	if err != nil {
		return err
	}

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
	streakColl := picksRef.Collection("streak")
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
		if streakPick != nil {
			ref := streakColl.NewDoc()
			if err := tx.Create(ref, streakPick); err != nil {
				return fmt.Errorf("Transaction failed to create StreakPick: %v", err)
			}
		}
		return nil
	})
	bucket := csclient.Bucket(slate.BucketName)
	obj := bucket.Object("picks/" + slate.File)
	w := obj.NewWriter(ctx)
	defer w.Close()
	w.ObjectAttrs.ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

	outExcel, err := newExcelFile(ctx, suPicks, nsPicks, sdPicks, streakPick)
	if err != nil {
		return err
	}

	outExcel.Write(w)

	return nil
}

// GetModels returns the model requested by the given identifier string, or the most conservative model if an empty path is given.
func GetModels(ctx context.Context, path string) (map[string]*firestore.DocumentSnapshot, error) {

	latestTracker, err := fsclient.Collection("prediction_tracker").OrderBy("timestamp", firestore.Desc).Limit(1).Documents(ctx).Next()
	if err != nil {
		return nil, fmt.Errorf("Failed to get latest prediction tracker: %v", err)
	}

	modelPerfs := latestTracker.Ref.Collection("model_performance")
	models := make(map[string]*firestore.DocumentSnapshot)
	if path == "" {
		log.Printf("No model requested: calculating best models at the time of pick")

		// Best SUW for straight-up picks
		greatModel, err := modelPerfs.OrderBy("suw", firestore.Desc).Limit(1).Documents(ctx).Next()
		if err != nil {
			return nil, fmt.Errorf("Failed to get great model: %v", err)
		}
		models["StraightUp"] = greatModel

		// Lowest MAE for noisy spreads and superdogs
		bestModel, err := modelPerfs.OrderBy("mae", firestore.Asc).Limit(1).Documents(ctx).Next()
		if err != nil {
			return nil, fmt.Errorf("Failed to get best model: %v", err)
		}
		models["NoisySpread"] = bestModel
		models["Superdog"] = bestModel

		return models, nil
	}

	// Just give me whatever you ask for
	modelRef := fsclient.Doc(path)
	model, err := modelPerfs.Where("model", "==", modelRef).Limit(1).Documents(ctx).Next()
	if err != nil {
		return nil, fmt.Errorf("Failed to get model at path '%s': %v", path, err)
	}
	models["StraightUp"] = model
	models["NoisySpread"] = model
	models["Superdog"] = model

	return models, nil
}

// LookupStreakPick looks up the streak pick for a picker in Firestore
func LookupStreakPick(ctx context.Context, picker, season *firestore.DocumentRef, week int) (*StreakPick, error) {
	// NOTE: the streak prediction is performed for the previous week.
	streakPredictionDoc, err := fsclient.Collection("streak_predictions").Where("picker", "==", picker).Where("season", "==", season).Where("week", "==", week-1).Limit(1).Documents(ctx).Next()
	if err == iterator.Done {
		// no streak yet, but that's okay!
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("Failed getting streak prediction for picker '%s', season '%s', week %d: %v", picker.ID, season.ID, week, err)
	}
	var streakPrediction StreakPrediction
	if err := streakPredictionDoc.DataTo(&streakPrediction); err != nil {
		return nil, fmt.Errorf("Failed parsing streak prediction for picker '%s', season '%s', week %d: %v", picker.ID, season.ID, week, err)
	}
	streakPick := &StreakPick{Picks: streakPrediction.BestPick,
		PredictedProbability: streakPrediction.Probability,
		PredictedSpread:      streakPrediction.Spread}
	return streakPick, nil
}
