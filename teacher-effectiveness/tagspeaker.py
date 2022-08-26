import json
import pandas as pd


def getSpeakerTime(transcription_json):
    # segments are speaker segments, items are words and punctuations
    segments = transcription_json['results']['speaker_labels']['segments']

    # Build dictionary of just speaker and times the speak
    speaker_times = {}
    for i, segment in enumerate(segments):
        speaker_times[i] = []
        speaker_times[i].append(segment['speaker_label'])
        if len(segment['items']) > 0:
            speaker_times[i].append(float(segment['items'][0]['start_time']))
            speaker_times[i].append(float(segment['items'][-1]['end_time']))
            speaker_times[i].append('')
        else:
            speaker_times[i].append(None)
            speaker_times[i].append(None)
            speaker_times[i].append(None)
    return speaker_times

def tagSpeakers(transcription_json, speaker_times):
    items = transcription_json['results']['items']
    # Sort through items and classify them to speakers
    item_index = 0
    for k in range(len(speaker_times)):
        if speaker_times[k][1] == None:
            continue

        speaker_start = float(speaker_times[k][1])
        speaker_end = float(speaker_times[k][2])

        for i in range(item_index, len(items)):
            item = items[i]

            if 'start_time' in item:
                item_start = float(item['start_time'])
                item_end = float(item['end_time'])
                if (item_start >= speaker_start) and (item_end <= speaker_end):
                    if speaker_times[k][-1] == '':
                        speaker_times[k][-1] += item['alternatives'][0]['content']
                        item_index = i+1  #Start with the next index in the next round
                    else:
                        speaker_times[k][-1] += ' ' + item['alternatives'][0]['content']  
                        item_index = i+1
                elif item_start > speaker_end:
                    break  #Don't need to check after speaker's turn is done

            else:
                item_start = float(items[i-1]['start_time'])
                item_end = float(items[i-1]['end_time'])
                if (item_start >= speaker_start) and (item_end <= speaker_end):
                    speaker_times[k][-1] += item['alternatives'][0]['content']
                    item_index = i+1
    
    # Make dataframe
    df = pd.DataFrame.from_dict(speaker_times, orient='index', columns=['speaker','start_time', 'end_time', 'speech'])
    return df