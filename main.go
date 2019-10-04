package pickem4me

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/360EntSecGroup-Skylar/excelize"
	"gonum.org/v1/gonum/stat/distuv"
)

// projectID is supplied by the environment, set automatically in GCF
var projectID = os.Getenv("GCP_PROJECT")

// fsclient is a lazily initialized firestore client
var fsclient *firestore.Client

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

// Slate represents a slate in Firestore.
type Slate struct {
	BucketName string                 `firestore:"bucket_name"`
	Created    time.Time              `firestore:"created"`
	File       string                 `firestore:"file"`
	Season     *firestore.DocumentRef `firestore:"season"`
	Week       int                    `firestore:"week"`
}

// SlateGame represents a game on a slate in Firestore.
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
	// PredictedSpread is the spread as predicted by the selected model.
	PredictedSpread float64 // not stored in Firestore
	// Pick is what the model suggests (including possible noisy spread adjustments).
	Pick *firestore.DocumentRef // not stored in Firestore
	// PredictedProbability is the probability the pick is correct (including possible noisy spread adjustments).
	PredictedProbability float64 // not stored in Firestore
	// Swap signals whether or not Luke's listing of the home/road teams need to be swapped.
	Swap bool // not stored in Firestore
	// NeutralDisagreement signals whether or not Luke's designation of a neutral site needs to be changed.
	NeutralDisagreement bool // not stored in Firestore
}

// Team is a simple representation of a team in Firestore.
type Team struct {
	School string `firestore:"school"`
	Name   string `firestore:"team"`
}

// SlateRow prints the game, the selection, and other columns about the game.
func (sg *SlateGame) SlateRow(ctx context.Context) ([]string, error) {
	if sg.Superdog {
		return sg.superdogRow(ctx)
	}
	return sg.gameRow(ctx)
}

func (sg *SlateGame) gameRow(ctx context.Context) ([]string, error) {
	// game, noise, pick, spread, notes, expected value
	output := make([]string, 6)

	homeDoc, err := sg.Home.Get(ctx)
	if err != nil {
		return nil, err
	}
	var homeTeam Team
	if err := homeDoc.DataTo(&homeTeam); err != nil {
		return nil, err
	}
	roadDoc, err := sg.Road.Get(ctx)
	if err != nil {
		return nil, err
	}
	var roadTeam Team
	if err := roadDoc.DataTo(&roadTeam); err != nil {
		return nil, err
	}

	var sb strings.Builder

	if sg.GOTW {
		sb.WriteRune('⭐')
	}

	if sg.Rank1 > 0 {
		sb.WriteString(fmt.Sprintf("#%d ", sg.Rank1))
	}

	sb.WriteString(roadTeam.School)

	if sg.NeutralSite || sg.NeutralDisagreement {
		sb.WriteString(" vs. ")
	} else {
		sb.WriteString(" @ ")
	}

	if sg.Rank2 > 0 {
		sb.WriteString(fmt.Sprintf("#%d ", sg.Rank2))
	}

	sb.WriteString(homeTeam.School)

	if sg.GOTW {
		sb.WriteRune('⭐')
	}

	output[0] = sb.String()

	if sg.NoisySpread != 0 {
		favorite := homeTeam
		ns := sg.NoisySpread
		if ns < 0 {
			favorite = roadTeam
			ns *= -1
		}
		output[1] = fmt.Sprintf("%s by ≥ %d", favorite.School, ns)
	}

	pickedTeam := homeTeam
	if sg.Pick.ID == sg.Road.ID {
		pickedTeam = roadTeam
	}

	if homeTeam.Name == roadTeam.Name {
		output[2] = pickedTeam.School
	} else {
		output[2] = pickedTeam.Name
	}

	output[3] = fmt.Sprintf("%0.1f", sg.PredictedSpread)

	sb.Reset()
	if math.Abs(sg.PredictedProbability) > .8 {
		sb.WriteString("Not even close.  ")
	}
	if sg.PredictedSpread >= 14 && sg.NoisySpread == 0 {
		sb.WriteString("Probabaly should have been noisy.  ")
	}
	if pickedTeam.School == "Michigan" {
		sb.WriteString("HARBAUGH!!!  ")
	}
	if sg.NeutralDisagreement {
		if sg.NeutralSite {
			sb.WriteString("You should know that this game isn't at a neutral site.  ")
		} else {
			sb.WriteString("You should know that this game is at a neutral site.  ")
		}
	} else if sg.Swap {
		sb.WriteString("You should know that the home and away teams are swapped.  ")
	}
	output[4] = strings.TrimSpace(sb.String())

	output[5] = fmt.Sprintf("%0.3f", float64(sg.Value)*math.Abs(sg.PredictedProbability))

	return output, nil
}

func (sg *SlateGame) superdogRow(ctx context.Context) ([]string, error) {

	underDoc, err := sg.Underdog.Get(ctx)
	if err != nil {
		return nil, err
	}
	var underdog Team
	if err := underDoc.DataTo(&underdog); err != nil {
		return nil, err
	}
	overDoc, err := sg.Overdog.Get(ctx)
	if err != nil {
		return nil, err
	}
	var overdog Team
	if err := overDoc.DataTo(&overdog); err != nil {
		return nil, err
	}

	// game, value, pick, spread, notes, expected value
	output := make([]string, 6)

	var sb strings.Builder

	sb.WriteString(underdog.School)
	sb.WriteString(" over ")
	sb.WriteString(overdog.School)

	output[0] = sb.String()
	sb.Reset()

	output[1] = fmt.Sprintf("(%d points)", sg.Value)

	if sg.Pick != nil {
		if underdog.Name != overdog.Name {
			output[2] = underdog.Name
		} else {
			output[2] = underdog.School
		}
	}

	output[3] = fmt.Sprintf("%0.1f", sg.PredictedSpread)

	output[5] = fmt.Sprintf("%0.4f", float64(sg.Value)*sg.PredictedProbability)

	return output, nil
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
	var pickedDog *SlateGame
	var bestValue float64

	// Manipulate in place using the index
	for i := range games {
		modelPred, swap, err := lookup(games[i].Home, games[i].Road)
		if err != nil {
			log.Printf("Failed looking up prediction: %v", err)
			return err
		}
		log.Printf("Found prediction for teams %s and %s: %v", games[i].Home.ID, games[i].Road.ID, *modelPred)

		// Remember that positive spread always favors home in the predictions.
		// It also needs to favor the overdog for calculating probability correctly.
		spread := modelPred.Spread
		if games[i].Superdog {
			if modelPred.Road.ID == games[i].Underdog.ID {
				spread *= -1
			}
		}

		target := games[i].NoisySpread
		if swap {
			target *= -1
		}
		prob := modelDist.CDF(spread - float64(target))

		// Update game
		games[i].Swap = swap
		games[i].NeutralDisagreement = games[i].NeutralSite != modelPred.Neutral
		games[i].PredictedSpread = modelPred.Spread
		games[i].PredictedProbability = prob

		// If it's a dog, wait on the pick
		if games[i].Superdog {
			ev := prob * float64(games[i].Value)
			if ev > bestValue {
				pickedDog = &games[i]
				bestValue = ev
			}
			continue
		}

		if prob >= .5 {
			games[i].Pick = modelPred.Home
		} else {
			games[i].Pick = modelPred.Road
		}
	}

	// Pick that dog!
	pickedDog.Pick = pickedDog.Underdog

	// Make an excel file in memory.
	outExcel := excelize.NewFile()
	sheetName := outExcel.GetSheetName(outExcel.GetActiveSheetIndex())
	// Write the header row
	outExcel.SetCellStr(sheetName, "A1", "GAME")
	outExcel.SetCellStr(sheetName, "B1", "Instruction")
	outExcel.SetCellStr(sheetName, "C1", "Your Selection")
	outExcel.SetCellStr(sheetName, "D1", "Predicted Spread")
	outExcel.SetCellStr(sheetName, "E1", "Notes")
	outExcel.SetCellStr(sheetName, "F1", "Expected Value")

	for _, game := range games {
		out, err := game.SlateRow(ctx)
		if err != nil {
			return fmt.Errorf("Failed making game output: %v", err)
		}
		for col, str := range out {
			colLetter := rune('A' + col)
			if game.Superdog {
				if col == 0 {
					colLetter++
				} else if col == 1 {
					continue
				}
			}
			index := fmt.Sprintf("%c%d", colLetter, game.Row)
			outExcel.SetCellStr(sheetName, index, str)
		}

	}

	// FIXME!
	outFile, err := os.Create("output_" + slate.File)
	if err != nil {
		return err
	}
	defer outFile.Close()
	outExcel.Write(outFile)

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
