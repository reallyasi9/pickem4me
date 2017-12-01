#!/usr/bin/env python3

import pandas as pd
import numpy as np
from urllib.request import urlopen
import bs4
import re
import yaml
import scipy.stats
import argparse
import logging
import os
import pprint

from googleapiclient import discovery
from oauth2client import client, tools
from oauth2client.file import Storage

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = "B1G Pick 'Em Picker"

def get_credentials(flags):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run_flow(flow, store, flags)
        print('Storing credentials to ' + credential_path)
    return credentials

def main(pred, res, slate, names, formats, model, output, level, dry, **flags):

    flags = argparse.Namespace(**flags)
    flags.logging_level = level

    credentials = get_credentials(flags)
    service = discovery.build('sheets', 'v4', credentials=credentials)

    with open(names) as names_file:
        names_dict = yaml.load(names_file)

    slate_df = download_slate(service, slate)
    logging.debug("\n%s", slate_df)

    pred_df = download_predictions(pred)
    logging.debug("\n%s", pred_df)

    models_df = download_models(res, names_dict)
    logging.debug("\n%s", models_df)

    slate_df, pred_df = fix_names(slate_df, pred_df, names_dict)
    logging.debug("\n%s", pred_df)
    logging.debug("\n%s", slate_df)

    slate_df = predict(slate_df, pred_df, models_df, model, names_dict)
    logging.debug("\n%s", slate_df)

    with open(formats) as format_file:
        format_dict = yaml.load(format_file)

    write_picks(service, slate, output, slate_df, format_dict, dry)



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
def download_slate(service, slate):

    range_names = ['A:A', 'B:B']
    result = service.spreadsheets().values().batchGet(
        spreadsheetId=slate, ranges=range_names, majorDimension="COLUMNS").execute()

    game_columns = result.get('valueRanges')[0]
    game_cells = game_columns.get('values')[0]

    game_regex = re.compile(r'(?:\*\*)?(?:\s*#\s*\d+\s+)?(.*?)\s+(vs\.?|@)\s+(?:#\s*\d+\s+)?(.*?)(?:\*\*)?$', re.IGNORECASE)
    games = [c for c in game_cells if c and game_regex.match(c)]

    home = []
    away = []
    neutral = []
    for g in games:
        matches = game_regex.match(g) # already know this works
        if matches:
            away.append(matches.group(1))
            home.append(matches.group(3))
            neutral.append(matches.group(2) != "@")

    gotw = [bool(re.search(r'\*\*', g)) for g in games]

    spread_columns = result.get('valueRanges')[1]
    spread_cells = spread_columns.get('values')[0]

    spread_text = [c for c in spread_cells if c and re.match(r'^Enter\s+(?!one)', c)]
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
                                    "neutral": neutral,
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
            logging.warn("{} slate teams missing from names map:\n{}".format(ha, slate_df.loc[non_match, ha]))
        slate_df.loc[non_match, 'proper_' + ha] = slate_df.loc[non_match, ha]

        pred_df[ha] = pred_df[ha].str.upper().str.replace(r'\.', '')
        pred_match = slate_df['proper_' + ha].isin(pred_df[ha])

        if (~pred_match).any():
            logging.warn("{} slate teams missing from predictions:\n{}".format(ha, slate_df.loc[~pred_match, ha]))

    # detect neutral site switcharoo
    pred_other_match = slate_df['neutral'] & slate_df['proper_home'].isin(pred_df['road']) & slate_df['proper_road'].isin(pred_df['home'])

    if pred_other_match.any():
        logging.warn("slate teams at neutral sites need to be flipped:\n{}".format(slate_df.loc[pred_other_match, ['home', 'road']]))
        old_home = slate_df['home'].copy()
        old_phome = slate_df['proper_home'].copy()
        slate_df.loc[pred_other_match, 'home'] = slate_df.loc[pred_other_match, 'road']
        slate_df.loc[pred_other_match, 'proper_home'] = slate_df.loc[pred_other_match, 'proper_road']
        slate_df.loc[pred_other_match, 'road'] = old_home.loc[pred_other_match]
        slate_df.loc[pred_other_match, 'proper_road'] = old_phome.loc[pred_other_match]

    logging.debug("Fixed slate:\n%s", slate_df)


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
    logging.debug("Merged dataset:\n%s", slate_df)

    noisy = slate_df['noisy_spread'] != 0

    slate_df['model'] = straight_model
    slate_df.loc[noisy, 'model'] = noisy_model

    slate_df['line'] = slate_df[straight_model]
    slate_df.loc[noisy, 'line'] = slate_df.loc[noisy, noisy_model]

    slate_df['bias'] = models_df.loc[straight_model, 'Bias']
    slate_df.loc[noisy, 'bias'] = models_df.loc[noisy_model, 'Bias']
    #slate_df.loc[slate_df['neutral_x'], 'bias'] /= 2 # for close games
    slate_df.loc[slate_df['neutral_x'], 'bias'] = 0

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
def write_picks(service, slate, output, slate_df, format_dict, dry):

    if output is None and not dry:
        logging.warn("No output spreadsheet ID given: performing dry run")
        dry = True

    slate_sheet = service.spreadsheets().get(spreadsheetId=slate).execute()
    slate_title = slate_sheet.get("properties").get("title")

    sheet_id = slate_sheet.get("sheets")[0].get("properties").get("sheetId")
    body = {"destinationSpreadsheetId": output}
    logging.debug("Requesting copyTo:\n%s", pprint.pformat(body))
    if not dry:
        output_sheet = service.spreadsheets().sheets().copyTo(
            spreadsheetId=slate, sheetId=sheet_id,
            body=body).execute()
        new_sheet_id = output_sheet.get("sheetId")
    else:
        new_sheet_id = None

    # Rename the sheet
    rename_sheet_body = {
        'requests': [
            {
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': new_sheet_id,
                        'title': slate_title
                    },
                    'fields': 'title'
                }
            }
        ]
    }
    logging.debug("Requesting:\n%s", pprint.pformat(rename_sheet_body))
    if not dry:
        new_sheet = service.spreadsheets().batchUpdate(spreadsheetId=output, body=rename_sheet_body).execute()

    update_data = []
    # titles
    update_data.append({
        "range": "'{}'!D1:J1".format(slate_title),
        "values": [["Probability of Correct Pick",
                    "Predicted Margin",
                    "Notes",
                    "Model",
                    "Winner",
                    "Spread",
                    "Correct"]],
        "majorDimension": "ROWS"
    })

    # picks
    update_data.append({
        "range": "'{}'!C2".format(slate_title),
        "values": [slate_df['pick'].fillna('').tolist()],
        "majorDimension": "COLUMNS"
    })

    # probabilities
    update_data.append({
        "range": "'{}'!D2".format(slate_title),
        "values": [slate_df['prob'].fillna('').tolist()],
        "majorDimension": "COLUMNS"
    })

    # lines
    update_data.append({
        "range": "'{}'!E2".format(slate_title),
        "values": [slate_df['debiased_line'].fillna('').tolist()],
        "majorDimension": "COLUMNS"
    })

    # notes
    slate_df['notes'] = ""
    noisy = (abs(slate_df['debiased_line']) >= 14) & (slate_df['noisy_spread'] == 0)
    slate_df.loc[noisy, 'notes'] = "Probably should have been a noisy spread.  " + slate_df.loc[noisy, 'notes']
    far = slate_df['prob'] >= .8
    slate_df.loc[far, 'notes'] = "Not even close.  " + slate_df.loc[far, 'notes']

    logging.info("Making the following picks:\n%s", slate_df[['proper_road', 'proper_home', 'noisy_spread', 'pick', 'prob', 'debiased_line', 'notes', 'model']])

    update_data.append({
        "range": "'{}'!F2".format(slate_title),
        "values": [slate_df['notes'].tolist()],
        "majorDimension": "COLUMNS"
    })

    update_data.append({
        "range": "'{}'!G2".format(slate_title),
        "values": [slate_df['model'].tolist()],
        "majorDimension": "COLUMNS"
    })

    logging.debug("Requesting batchUpdate of spreadsheetId[%s]:\n%s", output, pprint.pformat({'valueInputOption': "RAW", 'data': update_data}))
    body = {
        'valueInputOption': "RAW",
        'data': update_data
    }
    logging.debug("Requesting:\n%s", pprint.pformat(body))
    if not dry:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=output,
            body=body).execute()

    # For each pick, determine if we should update the style or not
    requests = []
    # Note: _x comes from the merge with the probs DF, which also had
    # 'home' and 'road' columns
    for i, row in enumerate(slate_df[['home_x', 'road_x', 'pick']].itertuples(index=False)):
        if row.pick not in format_dict:
            continue
        format = {"range": {"sheetId": new_sheet_id,
                            "startRowIndex": i+1,
                            "startColumnIndex": 2,
                            "endRowIndex": i+2,
                            "endColumnIndex": 3},
                  "fields": "*"}
        if row.pick == row.home_x:
            format['rows'] = [{"values": [format_dict[row.pick]['home']]}]
        else:
            format['rows'] = [{"values": [format_dict[row.pick]['road']]}]
        requests.append({"updateCells": format})

    body = {"requests": requests,
          "includeSpreadsheetInResponse": False}
    logging.debug("Requesting:\n%s", pprint.pformat(body))

    if not dry:
        service.spreadsheets().batchUpdate(
            spreadsheetId=output,
            body=body
        ).execute()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Make your picks for you!", parents=[tools.argparser])

    parser.add_argument('--slate', '-s', help="Google Sheets object ID of this week's slate",
                        metavar="SHEET_ID",
                        required=True)

    parser.add_argument('--output', '-o', help="Google Sheets object ID of output sheet",
                        metavar="SHEET_ID")

    parser.add_argument('--pred', '-p', help="URL of NCAA predictions file",
                        metavar="URL",
                        default="http://www.thepredictiontracker.com/ncaapredictions.csv")

    parser.add_argument('--res', '-r', help="URL containing HTML table of model performance and results",
                        metavar="URL",
                        default="http://www.thepredictiontracker.com/ncaaresults.php")

    parser.add_argument('--names', '-n', help="File containing translation from slate names to prediction names",
                        metavar="YAML",
                        default="names.yaml")

    parser.add_argument('--formats', '-f', help="File containing formats for certain special cells",
                        metavar="YAML",
                        default="formats.yaml")

    parser.add_argument('--model', '-m', help="Model name to use (typically begins with 'line')",
                        metavar="MODEL_NAME",
                        default='line')

    parser.add_argument('--level', '-l', help="Logging level",
                        metavar="LEVEL",
                        default='INFO', choices=logging._levelToName.values())

    parser.add_argument('--dry', '-d', help='Dry run (do not write output to Google Sheets)',
                        action="store_true")

    args = vars(parser.parse_args())

    logging.getLogger().setLevel(args['level'])

    main(**args)
