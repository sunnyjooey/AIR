import pytest
import sys
import pandas

from helpers import *
from tagspeaker import *
from video import getEmotion, process_video

@pytest.mark.unit
def test_process_video(ROOT_DIR):
	file_key = "testing"
	sys.path.append(ROOT_DIR)
	try:
		emotion = process_video(file_key, "mov")
	except:
		pytest.fail("Cannot get transcript")

	assert type(emotion) is pandas.core.frame.DataFrame
