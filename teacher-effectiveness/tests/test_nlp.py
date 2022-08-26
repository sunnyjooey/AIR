import pytest
import sys

from helpers import *
from nlp import *
@pytest.mark.unit
def test_getTranscript(ROOT_DIR):
    try:
        sys.path.append(ROOT_DIR)
        file_key = "SDS_team"
        transcript_json = getTranscript("transcription/{}.json".format(file_key))
    except:
        pytest.fail("Cannot get transcript")

    assert type(transcript_json) is dict


def test_process_nlp(ROOT_DIR):
    sys.path.append(ROOT_DIR)
    file_key = "SDS_team"
    try:
        _ = process_nlp(file_key)
    except:
        pytest.fail('Cannot process nlp modules')

    
