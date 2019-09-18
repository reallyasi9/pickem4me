import google.cloud.exceptions
from google.cloud import firestore
from google.cloud import error_reporting
from google.cloud import storage
from absl import logging
from absl import flags
from absl import app
import sys

FLAGS = flags.FLAGS

flags.DEFINE_string("slate", None, "Firestore path to slate document")
flags.DEFINE_string("tracker", None, "Firestore path to prediction tracker document")
flags.DEFINE_string("model", None, "Name of model to use (starts with 'line', defaults to best model for each of the pick categories)")

FS = firestore.Client()
ER = error_reporting.Client()
CS = storage.Client()


def get_best_models(tracker_ref):
    best_picker = next(tracker_ref.collection("modelperformance").order_by("pct_correct", direction="DESCENDING").limit(1).stream())
    best_spread = next(tracker_ref.collection("modelperformance").order_by("std_dev", direction="DESCENDING").limit(1).stream())
    best_dogger = next(tracker_ref.collection("modelperformance").order_by("pct_against_spread", direction="DESCENDING").limit(1).stream())

    return best_picker.to_dict()["model_id"], best_spread.to_dict()["model_id"], best_dogger.to_dict()["model_id"]



def pickem(argv):
    # Read slate
    slate_path = FLAGS.slate
    if slate_path is None:
        slates_ref = FS.collection("slates").order_by("timestamp", direction="DESCENDING").limit(1)
        slate = next(slates_ref.stream())
        slate_path = slate.reference.path
    
    logging.info("reading slate %s", slate_path)
    slate_ref = FS.document(slate_path)

    # Read predictions
    tracker_path = FLAGS.tracker
    if tracker_path is None:
        trackers_ref = FS.collection("prediction_tracker").order_by("timestamp", direction="DESCENDING").limit(1)
        tracker = next(trackers_ref.stream())
        tracker_path = tracker.reference.path
    
    logging.info("reading prediction tracker %s", tracker_path)
    tracker_ref = FS.document(tracker_path)

    # Get the model parameters
    use_model = FLAGS.model
    if use_model is None or use_model == "best":
        use_models = get_best_models(tracker_ref)
    else:
        use_models = [use_model, use_model, use_model]  # all the same for picks, spreads, and dogs
    
    logging.info("using trackers %s", str(use_models))
    # perf_ref = tracker_ref.collection("modelperformance").where("model_id", "==", use_model)

    games_ref = slate_ref.collection("games")
    for game in games_ref.stream():
        print(game.id, game.to_dict())


if __name__ == "__main__":

    logging.info("Function triggered with arguments: %s", str(sys.argv[1:]))
    app.run(pickem)
