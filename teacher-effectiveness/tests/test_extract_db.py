import pytest
import sys
import pandas
from helpers import *

@pytest.mark.unit
def test_extractTable(ROOT_DIR):
    table = filterTable('test','10')
    assert type(table) is pandas.core.frame.DataFrame

    yellingTable = extractFeatureTable(table['username'][0], table['class_timestamp'][0],'yelling')
    assert type(yellingTable) is pandas.core.frame.DataFrame

    emotionTable = extractFeatureTable('trang',table['class_timestamp'][0],'emotion')
    assert('emotion' and 'emotion_prob' in emotionTable.columns.tolist())

    jargonTable = extractFeatureTable('trang',table['class_timestamp'][0], 'jargon')
    assert('word' and 'tf' in jargonTable.columns.tolist())

    unofficialTable = extractFeatureTable('trang', table['class_timestamp'][0], 'unofficial')
    assert ('word' and 'tf' in unofficialTable.columns.tolist())
