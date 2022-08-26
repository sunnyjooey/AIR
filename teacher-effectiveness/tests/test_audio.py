import pytest
import sys
import pandas

from helpers import *
from tagspeaker import *
from audio import *
from nlp import *

@pytest.mark.unit
def test_process_audio(ROOT_DIR):
    file_key = "SDS_team"
    sys.path.append(ROOT_DIR)
    try:
        speaker_times = getSpeakerTime(getTranscript("transcription/{}.json".format(file_key)))
        speaker_tagged = tagSpeakers(getTranscript("transcription/{}.json".format(file_key)), speaker_times)
    except:
        pytest.fail("Cannot tag speakers")
    assert type(speaker_tagged) is pandas.core.frame.DataFrame

    try:
        yelling, laughter, applause = process_audio(file_key, speaker_tagged, step = 20)
        #yelling, laughter= process_audio(file_key, speaker_tagged)
    except:
        pytest.fail("Cannot get transcript")

    assert type(yelling) is pandas.core.frame.DataFrame
    assert type(laughter) is pandas.core.frame.DataFrame
    assert type(applause) is pandas.core.frame.DataFrame
