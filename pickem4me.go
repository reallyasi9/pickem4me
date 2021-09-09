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

	bpefs "github.com/reallyasi9/b1gpickem/firestore"
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

	// StraightModel is a path to a model to use when picking striaght-up picks (empty value means use the best model possible)
	StraightModel string `json:"straightModel"`

	// NoisySpreadModel is a path to a model to use when picking noisy spread picks (empty value means use the best model possible)
	NoisySpreadModel string `json:"noisySpreadModel"`

	// SuperdogModel is a path to a model to use when picking superdog picks (empty value means use the best model possible)
	SuperdogModel string `json:"superdogModel"`

	// DryRun tells the code to print what would be written, but not two create the excel output.
	DryRun bool `json:"dryrun,omitempty"`
}

// Model is a collection of performance metrics, predictions, and a distribution.
type Model struct {
	Performance    bpefs.ModelPerformance
	Predictions    []bpefs.Prediction
	Distribution   distuv.Normal
	PredictionRefs []*firestore.DocumentRef

	homeLookup map[string]int
	roadLookup map[string]int
}

// Lookup prediction by home and road teams, and whether or not to swap them for the slate.
func (m *Model) Lookup(home, road *firestore.DocumentRef) (*bpefs.Prediction, *firestore.DocumentRef, bool, error) {
	if m.homeLookup == nil || m.roadLookup == nil {
		// Make a lookup table for home and road teams
		m.homeLookup = make(map[string]int)
		m.roadLookup = make(map[string]int)
		for i, model := range m.Predictions {
			m.homeLookup[model.HomeTeam.ID] = i
			m.roadLookup[model.AwayTeam.ID] = i
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
			return nil, nil, false, fmt.Errorf("home team '%s' not in predicted game", home.ID)
		}
	}

	// if home is where it should be, road should be found as well
	if !maybeSwap {
		rr, ok = m.roadLookup[road.ID]
		if !ok {
			return nil, nil, false, fmt.Errorf("road team '%s' not in predicted game", road.ID)
		}
	} else {
		// road should be in home list
		rr, ok = m.homeLookup[road.ID]
		if !ok {
			return nil, nil, false, fmt.Errorf("road->home team '%s' not in predicted game", road.ID)
		}
	}

	// now check if the predicted games are the same (i.e., home is actually playing road!)
	if hr != rr {
		return nil, nil, false, fmt.Errorf("home '%s' and road '%s' not playing each other", home.ID, road.ID)
	}

	return &(m.Predictions[hr]), m.PredictionRefs[hr], maybeSwap, nil
}

func init() {
	ctx := context.Background()
	var err error
	fsclient, err = firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("failed making Firestore client: %v", err)
		panic(err)
	}
	csclient, err = storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed making Cloud Storage client: %v", err)
		panic(err)
	}
}

// PickEm consumes a Pub/Sub message.
func PickEm(ctx context.Context, m PubSubMessage) error {
	var pem PickEmMessage
	err := json.Unmarshal(m.Data, &pem)
	if err != nil {
		log.Printf("json.Unmarshal: %v", err)
		return err
	}

	// Get the slate
	slateDoc, err := fsclient.Doc(pem.Slate).Get(ctx)
	if err != nil {
		log.Printf("Failed getting slate '%s': %v", pem.Slate, err)
		return err
	}
	var slate bpefs.Slate
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
	games := make([]bpefs.Game, len(gameDocs))
	for i, doc := range gameDocs {
		var game bpefs.Game
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
	var picker bpefs.Picker
	if err := pickerDoc.DataTo(&picker); err != nil {
		log.Printf("Failed parsing picker '%s': %v", pem.Picker, err)
		return err
	}
	log.Printf("Got picker '%s': %v", pickerDoc.Ref.ID, picker)

	// Figure out the models to use
	modelPerfDocs, err := GetModels(ctx, pem.StraightModel, pem.NoisySpreadModel, pem.SuperdogModel)
	if err != nil {
		log.Printf("Failed getting models: %v", err)
		return err
	}

	models := make(map[string]Model)
	for gameType, doc := range modelPerfDocs {
		var modelPerf bpefs.ModelPerformance
		if err := doc.DataTo(&modelPerf); err != nil {
			log.Printf("Failed parsing model performance '%v': %v", doc, err)
			return err
		}
		log.Printf("Got model performance for '%s': %v", doc.Ref.ID, modelPerf)

		predictionDocs, err := doc.Ref.Collection("predictions").Documents(ctx).GetAll()
		if err != nil {
			log.Printf("Failed getting predictions from model '%v': %v", doc, err)
			return err
		}
		preds := make([]bpefs.Prediction, len(predictionDocs))
		predRefs := make([]*firestore.DocumentRef, len(predictionDocs))
		for i, doc := range predictionDocs {
			predRefs[i] = doc.Ref
			var prediction bpefs.Prediction
			if err := doc.DataTo(&prediction); err != nil {
				log.Printf("Failed parsing prediction '%s': %v", doc.Ref.ID, err)
				return err
			}
			log.Printf("Got prediction '%s': %v", doc.Ref.ID, prediction)
			preds[i] = prediction
		}

		dist := distuv.Normal{
			Mu:    modelPerf.Bias,
			Sigma: modelPerf.StdDev,
		}

		models[gameType] = Model{
			Predictions:    preds,
			Performance:    modelPerf,
			Distribution:   dist,
			PredictionRefs: predRefs,
		}
	}

	// Pick-a-dog
	var pickedDog *bpefs.SuperDogPick
	var bestValue float64

	// Make picks separate from slate games
	suPicks := make([]*bpefs.StraightUpPick, 0)
	nsPicks := make([]*bpefs.NoisySpreadPick, 0)
	sdPicks := make([]*bpefs.SuperDogPick, 0)

	for _, game := range games {
		gameType := "StraightUp"
		if game.Superdog {
			gameType = "Superdog"
		}
		if game.NoisySpread != 0 {
			gameType = "NoisySpread"
		}

		model := models[gameType]
		modelPred, predRef, swap, err := model.Lookup(game.HomeTeam, game.AwayTeam)
		if err != nil {
			log.Printf("Failed looking up prediction: %v", err)
			return err
		}
		log.Printf("Found prediction for teams %s and %s: %v", game.HomeTeam.ID, game.AwayTeam.ID, *modelPred)

		// The the model spread is always relative to the _correct_ home team (as understood by the model).
		// That means the target, which is relative to the home team of the slate, might need ot be swapped as well.
		spread := modelPred.Spread
		target := game.NoisySpread
		if swap {
			spread *= -1 // "spread" is now relative to the slate home team.
			target *= -1 // "target" is now relative to the true home team.
		}
		// This is tricky because prob needs to be relative to the true home team.
		// The model already calculates the spread based on the true home team.
		// The target (noisy spread) was just flipped if necessary, so it is also relative to the true home team.
		prob := model.Distribution.CDF(modelPred.Spread - float64(target))

		// Disagreement over neutral site?
		neutralDisagreement := game.NeutralSite != modelPred.NeutralSite

		// Update game
		// Note that the game that is written reflects the slate, not the "truth".
		// That means home and away teams and spreads might need to be swapped later.
		if game.Superdog {
			// The probability is always relative to the true home team.
			// The "underdog" and "overdog" is parsed from the slate. They are taken to be correct as-is.
			// The way that superdogs are parsed means the underdog is always considered the away team in the slate.
			// So from the game's point of view, prob is the probability that the overdog wins.
			// That means the probability has to be inverted for display purposes (unless the home team actually is the underdog, in which case we are okay).
			if !swap {
				prob = 1 - prob
			} else {
				// ...but it also means that the spread has to be set back to the original value. Ugh.
				spread *= -1
			}
			sdp := bpefs.SuperDogPick{
				Underdog:             game.Underdog,
				Overdog:              game.Overdog,
				UnderdogRank:         game.AwayRank,
				OverdogRank:          game.HomeRank,
				Value:                game.Value,
				NeutralSite:          game.NeutralSite != neutralDisagreement, // logic: 0,0->0, 0,1->1, 1,0->1, 1,1->0
				NeutralDisagreement:  neutralDisagreement,
				HomeAwaySwap:         swap,
				Pick:                 nil, // hold off on picking superdogs
				PredictedSpread:      spread,
				PredictedProbability: prob,
				ModeledGame:          predRef,
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

		pick := modelPred.HomeTeam
		if prob < 0.5 {
			pick = modelPred.AwayTeam
		}
		if game.NoisySpread != 0 {
			nsPicks = append(nsPicks, &bpefs.NoisySpreadPick{
				HomeTeam:             modelPred.HomeTeam,
				AwayTeam:             modelPred.AwayTeam,
				AwayRank:             game.AwayRank,
				HomeRank:             game.HomeRank,
				NoisySpread:          game.NoisySpread,
				NeutralSite:          game.NeutralSite != neutralDisagreement, // logic: 0,0->0, 0,1->1, 1,0->1, 1,1->0
				NeutralDisagreement:  neutralDisagreement,
				HomeAwaySwap:         swap,
				Pick:                 pick,
				PredictedSpread:      spread,
				PredictedProbability: prob,
				ModeledGame:          predRef,
				Row:                  game.Row,
			})
			continue
		}
		suPicks = append(suPicks, &bpefs.StraightUpPick{
			HomeTeam:             modelPred.HomeTeam,
			AwayTeam:             modelPred.AwayTeam,
			AwayRank:             game.AwayRank,
			HomeRank:             game.HomeRank,
			GOTW:                 game.GOTW,
			NeutralSite:          game.NeutralSite != neutralDisagreement, // logic: 0,0->0, 0,1->1, 1,0->1, 1,1->0
			NeutralDisagreement:  neutralDisagreement,
			HomeAwaySwap:         swap,
			Pick:                 pick,
			PredictedSpread:      spread,
			PredictedProbability: prob,
			ModeledGame:          predRef,
			Row:                  game.Row,
		})
	}

	// Pick that dog!  But only if dogs are still being picked!
	if pickedDog != nil {
		pickedDog.Pick = pickedDog.Underdog
	}

	// Finally look up streak
	streakPick, err := LookupStreakPick(ctx, pickerDoc.Ref, slate.Season, slate.Week)
	if err != nil {
		return err
	}

	if pem.DryRun {
		f, err := os.CreateTemp(".", "dryrun.*.xlsx")
		if err != nil {
			return err
		}
		defer f.Close()

		log.Printf("DRYRUN: writing Excel output to path %s", f.Name())

		outExcel, err := newExcelFile(ctx, suPicks, nsPicks, sdPicks, streakPick)
		if err != nil {
			return err
		}

		outExcel.Write(f)
		return nil
	}

	// With picks in place, write to Firestore
	err = fsclient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		picksColl := fsclient.Collection("picks")
		picksRef := picksColl.NewDoc()
		if err := tx.Create(picksRef, &bpefs.Picks{
			Season: slate.Season,
			Week:   slate.Week,
			Picker: pickerDoc.Ref,
		}); err != nil {
			return fmt.Errorf("transaction failed to create picks: %v", err)
		}

		suColl := picksRef.Collection("straight_up")
		nsColl := picksRef.Collection("noisy_spread")
		sdColl := picksRef.Collection("superdog")
		streakColl := picksRef.Collection("streak")
		for _, pick := range suPicks {
			ref := suColl.NewDoc()
			if err := tx.Create(ref, pick); err != nil {
				return fmt.Errorf("transaction failed to create StraightUpPick: %v", err)
			}
		}
		for _, pick := range nsPicks {
			ref := nsColl.NewDoc()
			if err := tx.Create(ref, pick); err != nil {
				return fmt.Errorf("transaction failed to create NoisySpreadPick: %v", err)
			}
		}
		for _, pick := range sdPicks {
			ref := sdColl.NewDoc()
			if err := tx.Create(ref, pick); err != nil {
				return fmt.Errorf("transaction failed to create SuperDogPick: %v", err)
			}
		}
		if streakPick != nil {
			ref := streakColl.NewDoc()
			if err := tx.Create(ref, streakPick); err != nil {
				return fmt.Errorf("transaction failed to create StreakPick: %v", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	bucket := csclient.Bucket(slate.Bucket)
	obj := bucket.Object("picks/" + slate.FileName)
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
func GetModels(ctx context.Context, suPath, nsPath, sdPath string) (map[string]*firestore.DocumentSnapshot, error) {

	latestTracker, err := fsclient.Collection("prediction_tracker").OrderBy("timestamp", firestore.Desc).Limit(1).Documents(ctx).Next()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest prediction tracker: %v", err)
	}

	modelPerfs := latestTracker.Ref.Collection("model_performance")
	models := make(map[string]*firestore.DocumentSnapshot)

	search := func(path, orderBy string, dir firestore.Direction) (*firestore.DocumentSnapshot, error) {
		if path == "" {
			log.Printf("No model requested: finding model by %s (%v) at the time of pick", orderBy, dir)

			greatModel, err := modelPerfs.OrderBy(orderBy, dir).Limit(1).Documents(ctx).Next()
			if err != nil {
				return nil, fmt.Errorf("failed to get best model: %v", err)
			}
			return greatModel, nil
		}
		modelRef := fsclient.Doc(path)
		model, err := modelPerfs.Where("model", "==", modelRef).Limit(1).Documents(ctx).Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get model at path '%s': %v", path, err)
		}
		return model, nil
	}

	// Greatest straight-up wins for straight-up picks
	m, err := search(suPath, "suw", firestore.Desc)
	if err != nil {
		return nil, fmt.Errorf("GetModels: failed to get model for straight-up picks: %v", err)
	}
	models["StraightUp"] = m

	// Lowest mean absolute error for noisy spread picks
	m, err = search(nsPath, "mae", firestore.Asc)
	if err != nil {
		return nil, fmt.Errorf("GetModels: failed to get model for noisy spread picks: %v", err)
	}
	models["NoisySpread"] = m

	// Lowest mean absolute error for superdog picks
	if sdPath == "" {
		log.Print("Superdog model not given: using noisy spread model instead")
		sdPath = nsPath
	}
	m, err = search(sdPath, "mae", firestore.Asc)
	if err != nil {
		return nil, fmt.Errorf("GetModels: failed to get model for superdog picks: %v", err)
	}
	models["Superdog"] = m

	return models, nil
}

// LookupStreakPick looks up the streak pick for a picker in Firestore
func LookupStreakPick(ctx context.Context, picker, season *firestore.DocumentRef, week int) (*bpefs.StreakPick, error) {
	// NOTE: the streak prediction is performed for _this_ week.
	streakPredictionDoc, err := fsclient.Collection("streak_predictions").Where("picker", "==", picker).Where("season", "==", season).Where("week", "==", week).Limit(1).Documents(ctx).Next()
	if err == iterator.Done {
		// no streak yet, but that's okay!
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed getting streak prediction for picker '%s', season '%s', week %d: %v", picker.ID, season.ID, week, err)
	}
	var streakPrediction bpefs.StreakPredictions
	if err := streakPredictionDoc.DataTo(&streakPrediction); err != nil {
		return nil, fmt.Errorf("failed parsing streak prediction for picker '%s', season '%s', week %d: %v", picker.ID, season.ID, week, err)
	}
	streakPick := &bpefs.StreakPick{Picks: streakPrediction.BestPick,
		PredictedProbability: streakPrediction.Probability,
		PredictedSpread:      streakPrediction.Spread}
	return streakPick, nil
}
