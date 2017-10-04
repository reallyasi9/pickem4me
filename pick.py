#!/usr/bin/env python3

import pandas as pd
import numpy as np
from urllib.request import urlopen
import bs4
import openpyxl
import re
import yaml
import scipy.stats
import argparse
import logging


def main(pred, res, slate, names, model, output, debug):

    logging.getLogger().setLevel(debug)

    pred_df = download_predictions(pred)
    logging.debug(pred_df)

    with open(names) as names_file:
        names_dict = yaml.load(names_file)
    models_df = download_models(res, names_dict)
    logging.debug(models_df)

    slate_df = parse_slate(slate)
    logging.debug(slate_df)

    slate_df, pred_df = fix_names(slate_df, pred_df, names_dict)
    logging.debug(pred_df)
    logging.debug(slate_df)

    slate_df = predict(slate_df, pred_df, models_df, model, names_dict)
    logging.debug(slate_df)

    write_picks(slate_df, slate, output)



## Read in the current line and computer rankings from the Internet.
def download_predictions(pred):
    with urlopen(pred) as pred_file:
        pred_df = pd.read_csv(pred_file)
    return pred_df


## Read the current model perfomance from the Internet.
def download_models(res, names):
    reverse_dict = {val: key for key, val in names['models'].items()}
    with urlopen(res) as results_file:
        bs = bs4.BeautifulSoup(results_file, "lxml")
        results_df = pd.read_html(str(bs), attrs={'class': 'results_table'}, header=0)
        results_df = results_df[0]
        results_df['System'] = results_df['System'].map(reverse_dict)
        results_df['std_dev'] = np.sqrt(results_df['Mean Square Error'] - results_df['Bias'].pow(2))
        results_df = results_df[pd.notnull(results_df['System'])]
        return results_df.set_index('System')


## Read in the current slate.
def parse_slate(slate):
    wb = openpyxl.load_workbook(slate)
    ws = wb.active
    game_cells = ws['A']
    game_regex = re.compile(r'(?:\*\*)?(?:#\d+\s+)?(.*?)\s+(?:vs\.?|@)\s+(?:#\d+\s+)?(.*?)(?:\*\*)?$', re.IGNORECASE)
    games = [c.value for c in game_cells if c.value and game_regex.match(c.value)]

    home = []
    away = []
    for g in games:
        matches = game_regex.match(g)
        if matches:
            away.append(matches.group(1))
            home.append(matches.group(2))

    gotw = [bool(re.search(r'\*\*', g)) for g in games]

    spread_cells = ws['B']
    spread_text = [c.value for c in spread_cells if c.value and re.match(r'^Enter\s+(?!one)', c.value)]
    spread_regex = re.compile(r'^Enter\s+(.*?)\s+iff.*?(\d+)\s+points', re.IGNORECASE)
    spread_favorites = []
    noisy_spreads = []
    for s in spread_text:
        matches = spread_regex.match(s)
        if matches:
            spread_favorites.append(matches.group(1))
            noisy_spreads.append(int(matches.group(2)))
        else:
            spread_favorites.append(None)
            noisy_spreads.append(0)

    slate_df = pd.DataFrame(data = {"road": away,
                                    "home": home,
                                    "gotw": gotw,
                                    "noisy_favorite": spread_favorites,
                                    "noisy_spread": noisy_spreads})

    slate_df.loc[slate_df['noisy_favorite'] == slate_df['road'], 'noisy_spread'] *= -1

    return slate_df


## Get proper team names
def fix_names(slate_df, pred_df, names_dict):

    reverse_dict = {val: key.upper() for key, val in names_dict['teams'].items()}

    for ha in ['home', 'road']:
        slate_df['proper_' + ha] = slate_df[ha].map(reverse_dict)
        non_match = pd.isnull(slate_df['proper_' + ha])
        if non_match.any():
            logging.warn("{} slate teams missing from names map: {}".format(ha, slate_df.loc[non_match, ha]))
        slate_df.loc[non_match, 'proper_' + ha] = slate_df.loc[non_match, ha]

        pred_df[ha] = pred_df[ha].str.upper().str.replace(r'\.', '')
        pred_match = slate_df['proper_' + ha].isin(pred_df[ha])
        if (~pred_match).any():
            logging.warn("{} slate teams missing from predictions: {}".format(ha, slate_df.loc[~pred_match, ha]))

    return slate_df, pred_df


## Make picks
def predict(slate_df, pred_df, models_df, model, names_dict):

    straight_model = model
    noisy_model = model
    logging.info("using model {}".format(model))
    if model == 'best':
        straight_model = models_df.sort_values('Pct. Correct', ascending=False).index[0]
        noisy_model = models_df.sort_values('std_dev', ascending=True).index[0]
        logging.info("best straight pick model: {}".format(straight_model))
        logging.info("best noisy spread model: {}".format(noisy_model))

    slate_df = pd.merge(slate_df, pred_df, how='left',
                        left_on=['proper_home', 'proper_road'], right_on=['home', 'road'])

    noisy = slate_df['noisy_spread'] != 0

    slate_df['line'] = slate_df[straight_model]
    slate_df.loc[noisy, 'line'] = slate_df.loc[noisy, noisy_model]

    slate_df['bias'] = models_df.loc[straight_model, 'Bias']
    slate_df.loc[noisy, 'bias'] = models_df.loc[noisy_model, 'Bias']

    slate_df['std_dev'] = models_df.loc[straight_model, 'std_dev']
    slate_df.loc[noisy, 'std_dev'] = models_df.loc[noisy_model, 'std_dev']

    slate_df['debiased_line'] = slate_df['line'] - slate_df['bias']

    slate_df['prob'] = 1 - scipy.stats.norm.cdf(slate_df['noisy_spread'], loc=slate_df['debiased_line'],
                                                scale=slate_df['std_dev'])
    slate_df['pick'] = slate_df['proper_home']
    slate_df.loc[pd.isnull(slate_df['prob']), 'pick'] = None # no line available
    road_picks = slate_df['prob'] < .5
    slate_df.loc[road_picks, 'pick'] = slate_df.loc[road_picks, 'proper_road']
    slate_df.loc[road_picks, 'prob'] = 1 - slate_df.loc[road_picks, 'prob']

    names_lookup = {key.upper(): val for key, val in names_dict['teams'].items()}
    slate_df['pick'] = slate_df['pick'].map(names_lookup)

    return slate_df


## Write picks
def write_picks(slate_df, slate, output):
    wb = openpyxl.load_workbook(slate)
    ws = wb.active

    ws.cell(row=1, column=4, value="Probability of Correct Pick").font = openpyxl.styles.Font(bold=True)
    ws.cell(row=1, column=5, value="Predicted Margin").font = openpyxl.styles.Font(bold=True)
    ws.cell(row=1, column=6, value="Notes").font = openpyxl.styles.Font(bold=True)

    for i, row in enumerate(slate_df[['pick', 'prob', 'debiased_line']].itertuples(index=False)):
        logging.info("Pick: {}".format(row))
        ws.cell(row=i+2, column=3, value=row[0])
        ws.cell(row=i+2, column=4, value=row[1])
        ws.cell(row=i+2, column=5, value=row[2])

    wb.save(output)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Make your picks for you!")

    parser.add_argument('--slate', '-s', help="This week's slate",
                        required=True)

    parser.add_argument('--output', '-o', help="Output Excel file name",
                        required=True)

    parser.add_argument('--pred', '-p', help="URL of NCAA predictions file",
                        default="http://www.thepredictiontracker.com/ncaapredictions.csv")

    parser.add_argument('--res', '-r', help="URL containing HTML table of model performance and results",
                        default="http://www.thepredictiontracker.com/ncaaresults.php")

    parser.add_argument('--names', '-n', help="File containing translation from slate names to prediction names",
                        default="names.yaml")

    parser.add_argument('--model', '-m', help="Model name to use (typically begins with 'line')",
                        default='line')

    parser.add_argument('--debug', '-d', help="Debug level",
                        default='WARNING', choices=logging._levelToName.values())

    args = parser.parse_args()

    main(**vars(args))
