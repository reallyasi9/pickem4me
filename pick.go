package pickem4me

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
)

// Picks represents a collection of pickers' picks for the week.
type Picks struct {
	Season    *firestore.DocumentRef `firestore:"season"`
	Week      int                    `firestore:"week"`
	Timestamp time.Time              `firestore:"timestamp,serverTimestamp"`
	Picker    *firestore.DocumentRef `firestore:"picker"`
}

// StraightUpPick is a pick on a game with no spread.
type StraightUpPick struct {
	// Home is the true home team (not what Luke said).
	Home *firestore.DocumentRef `firestore:"home"`
	// Road is the true road team (not what Luke said).
	Road  *firestore.DocumentRef `firestore:"road"`
	Rank1 int                    `firestore:"rank1"`
	Rank2 int                    `firestore:"rank2"`
	GOTW  bool                   `firestore:"gotw"`
	// NeutralSite is the true neutral site nature of the game (not what Luke said).
	NeutralSite bool `firestore:"neutral_site"`
	// NeutralDisagreement is whether or not Luke lied to us about the neutral site of the game.
	NeutralDisagreement bool `firestore:"neutral_disagreement"`
	// Swap is whether or not Luke lied to us about who are the home and road teams.
	Swap bool `firestore:"swap"`
	// Pick is what the user picked, regardless of the model output.
	Pick *firestore.DocumentRef `firestore:"pick"`
	// PredictedSpread is the spread as predicted by the selected model.
	PredictedSpread float64 `firestore:"predicted_spread"`
	// PredictedProbability is the probability the pick is correct (including possible noisy spread adjustments).
	PredictedProbability float64 `firestore:"predicted_probability"`
	// ModeledGame is a reference to the spread from the model used to make the pick
	ModeledGame *firestore.DocumentRef `firestore:"modeled_game"`
	// Row is the row in the slate whence the pick originated.
	Row int `firestore:"row"`
}

// NoisySpreadPick is a pick on a noisy spread game.
type NoisySpreadPick struct {
	// Home is the true home team (not what Luke said).
	Home *firestore.DocumentRef `firestore:"home"`
	// Road is the true road team (not what Luke said).
	Road        *firestore.DocumentRef `firestore:"road"`
	Rank1       int                    `firestore:"rank1"`
	Rank2       int                    `firestore:"rank2"`
	NoisySpread int                    `firestore:"noisy_spread"`
	// NeutralSite is the true neutral site nature of the game (not what Luke said).
	NeutralSite bool `firestore:"neutral_site"`
	// NeutralDisagreement is whether or not Luke lied to us about the neutral site of the game.
	NeutralDisagreement bool `firestore:"neutral_disagreement"`
	// Swap is whether or not Luke lied to us about who are the home and road teams.
	Swap bool `firestore:"swap"`
	// Pick is what the user picked, regardless of the model output.
	Pick *firestore.DocumentRef `firestore:"pick"`
	// PredictedSpread is the spread as predicted by the selected model.
	PredictedSpread float64 `firestore:"predicted_spread"`
	// PredictedProbability is the probability the pick is correct (including possible noisy spread adjustments).
	PredictedProbability float64 `firestore:"predicted_probability"`
	// ModeledGame is a reference to the spread from the model used to make the pick
	ModeledGame *firestore.DocumentRef `firestore:"modeled_game"`
	// Row is the row in the slate whence the pick originated.
	Row int `firestore:"row"`
}

// SuperDogPick is a pick on a superdog spread game.
type SuperDogPick struct {
	// Underdog is what Luke called the underdog, regardless of model predictions.
	Underdog *firestore.DocumentRef `firestore:"underdog"`
	// Overdog is what Luke called the overdog, regardless of model predictions.
	Overdog *firestore.DocumentRef `firestore:"overdog"`
	Rank1   int                    `firestore:"rank1"`
	Rank2   int                    `firestore:"rank2"`
	Value   int                    `firestore:"value"`
	// NeutralSite is the true neutral site nature of the game (not what Luke said).
	NeutralSite bool `firestore:"neutral_site"`
	// NeutralDisagreement is whether or not Luke lied to us about the neutral site of the game.
	NeutralDisagreement bool `firestore:"neutral_disagreement"`
	// Swap is whether or not Luke lied to us about who are the home and road teams.
	Swap bool `firestore:"swap"`
	// Pick is what the user picked, regardless of the model output.
	// Is nil if this game was not picked.
	Pick *firestore.DocumentRef `firestore:"pick"`
	// PredictedSpread is the spread as predicted by the selected model.
	PredictedSpread float64 `firestore:"predicted_spread"`
	// PredictedProbability is the probability the pick is correct (including possible noisy spread adjustments).
	PredictedProbability float64 `firestore:"predicted_probability"`
	// ModeledGame is a reference to the spread from the model used to make the pick
	ModeledGame *firestore.DocumentRef `firestore:"modeled_game"`
	// Row is the row in the slate whence the pick originated.
	Row int `firestore:"row"`
}

// StreakPick is a pick for the streak.
type StreakPick struct {
	// Picks is what the user picked, regardless of the model output.
	// Note that there could be multiple picks per week.
	Picks []*firestore.DocumentRef `firestore:"picks"`
	// PredictedSpread is the spread of the remaining games in the optimal streak as predicted by the selected model.
	PredictedSpread float64 `firestore:"predicted_spread"`
	// PredictedProbability is the probability of beating the streak.
	PredictedProbability float64 `firestore:"predicted_probability"`
}

// SlateRow creates a row of strings for direct output to a slate spreadsheet.
func (sg StraightUpPick) SlateRow(ctx context.Context) ([]string, error) {
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

	if sg.NeutralSite {
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
	if pickedTeam.School == "Michigan" {
		sb.WriteString("HARBAUGH!!!\n")
	}
	if math.Abs(sg.PredictedProbability) > .8 {
		sb.WriteString("Not even close.\n")
	}
	if sg.PredictedSpread >= 14 {
		sb.WriteString("Probabaly should have been noisy.\n")
	}
	if sg.NeutralDisagreement {
		if sg.NeutralSite {
			sb.WriteString("NOTE:  This game is at a neutral site.\n")
		} else {
			sb.WriteString("NOTE:  This game isn't at a neutral site.\n")
		}
	} else if sg.Swap {
		sb.WriteString("NOTE:  The home and away teams are reversed from their actual values.\n")
	}
	output[4] = strings.Trim(sb.String(), "\n")

	value := 1.
	if sg.GOTW {
		value = 2.
	}
	output[5] = fmt.Sprintf("%0.3f", value*math.Abs(sg.PredictedProbability))

	return output, nil
}

// SlateRow creates a row of strings for direct output to a slate spreadsheet.
func (sg NoisySpreadPick) SlateRow(ctx context.Context) ([]string, error) {
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

	if sg.Rank1 > 0 {
		sb.WriteString(fmt.Sprintf("#%d ", sg.Rank1))
	}

	sb.WriteString(roadTeam.School)

	if sg.NeutralSite {
		sb.WriteString(" vs. ")
	} else {
		sb.WriteString(" @ ")
	}

	if sg.Rank2 > 0 {
		sb.WriteString(fmt.Sprintf("#%d ", sg.Rank2))
	}

	sb.WriteString(homeTeam.School)

	output[0] = sb.String()

	favorite := homeTeam
	ns := sg.NoisySpread
	if ns < 0 {
		favorite = roadTeam
		ns *= -1
	}
	output[1] = fmt.Sprintf("%s by ≥ %d", favorite.School, ns)

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
	if pickedTeam.School == "Michigan" {
		sb.WriteString("HARBAUGH!!!\n")
	}
	if math.Abs(sg.PredictedProbability) > .8 {
		sb.WriteString("Not even close.\n")
	}
	if sg.PredictedSpread < 14 {
		sb.WriteString("This one will be closer than you think.\n")
	}
	if sg.NeutralDisagreement {
		if sg.NeutralSite {
			sb.WriteString("NOTE:  This game is at a neutral site.\n")
		} else {
			sb.WriteString("NOTE:  This game isn't at a neutral site.\n")
		}
	} else if sg.Swap {
		sb.WriteString("NOTE:  The home and away teams are reversed from their actual values.\n")
	}
	output[4] = strings.Trim(sb.String(), "\n")

	output[5] = fmt.Sprintf("%0.3f", math.Abs(sg.PredictedProbability))

	return output, nil
}

// SlateRow creates a row of strings for direct output to a slate spreadsheet.
func (sg SuperDogPick) SlateRow(ctx context.Context) ([]string, error) {

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

	if sg.PredictedProbability > 0.5 {
		output[4] = "NOTE:  The \"underdog\" is favored to win!"
	}

	output[5] = fmt.Sprintf("%0.4f", float64(sg.Value)*sg.PredictedProbability)

	return output, nil
}

// SlateRow creates a row of strings for direct output to a slate spreadsheet.
// TODO: still not printing DDs correctly.
func (sg StreakPick) SlateRow(ctx context.Context) ([]string, error) {

	pickTeamDocs, err := fsclient.GetAll(ctx, sg.Picks)
	if err != nil {
		return nil, err
	}

	pickTeams := make([]*Team, len(pickTeamDocs))
	for i, doc := range pickTeamDocs {
		if err := doc.DataTo(pickTeams[i]); err != nil {
			return nil, err
		}
	}

	// nothing, instruction, pick, spread, notes, expected value
	output := make([]string, 6)

	output[1] = "BEAT THE STREAK!"

	output[2] = strings.Join(uniqueTeamNames(pickTeams), " + ")

	output[3] = fmt.Sprintf("%0.1f", sg.PredictedSpread)

	output[5] = fmt.Sprintf("%0.4f", sg.PredictedProbability)

	return output, nil
}

func uniqueTeamNames(teams []*Team) []string {
	uniqueNames := make([]string, len(teams))
	names := make(map[string]bool)
	useSchools := false
	for _, t := range teams {
		if _, exists := names[t.Name]; exists {
			useSchools = true
			break
		}
		names[t.Name] = true
	}
	for i, t := range teams {
		if useSchools {
			uniqueNames[i] = t.School
		} else {
			uniqueNames[i] = t.Name
		}
	}
	return uniqueNames
}
