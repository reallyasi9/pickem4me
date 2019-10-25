package pickem4me

import (
	"context"
	"fmt"
	"math"

	"github.com/360EntSecGroup-Skylar/excelize"
)

func addRow(ctx context.Context, outExcel *excelize.File, sheetName string, pick SlatePrinter, row int) error {
	out, err := pick.SlateRow(ctx)
	if err != nil {
		return fmt.Errorf("Failed making game output: %v", err)
	}
	for col, str := range out {
		colLetter := rune('A' + col)
		switch pick.(type) {
		case SuperDogPick:
			if col == 0 {
				colLetter++
			} else if col == 1 {
				continue
			}
		}
		index := fmt.Sprintf("%c%d", colLetter, row)
		outExcel.SetCellStr(sheetName, index, str)
	}
	return nil
}

func newExcelFile(ctx context.Context, suPicks []*StraightUpPick, nsPicks []*NoisySpreadPick, sdPicks []*SuperDogPick, streakPick *StreakPick) (*excelize.File, error) {
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

	lastPickRow := -1 // need to calculate where the BTS row is
	firstSDRow := -1

	for _, game := range suPicks {
		if game.Row > lastPickRow {
			lastPickRow = game.Row
		}
		if err := addRow(ctx, outExcel, sheetName, game, game.Row); err != nil {
			return nil, err
		}
	}

	for _, game := range nsPicks {
		if game.Row > lastPickRow {
			lastPickRow = game.Row
		}
		if err := addRow(ctx, outExcel, sheetName, game, game.Row); err != nil {
			return nil, err
		}
	}

	for _, game := range sdPicks {
		if game.Row < firstSDRow || firstSDRow < 0 {
			firstSDRow = game.Row
		}
		if err := addRow(ctx, outExcel, sheetName, game, game.Row); err != nil {
			return nil, err
		}
	}

	if streakPick != nil {
		// Between the picks and dogs, closer to the picks.
		row := int(math.Ceil(float64(lastPickRow) + float64(firstSDRow-lastPickRow)/2.))
		if err := addRow(ctx, outExcel, sheetName, streakPick, row); err != nil {
			return nil, err
		}
	}

	return outExcel, nil
}
