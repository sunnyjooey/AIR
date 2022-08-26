import json
import pandas as pd
import numpy as np
from helpers import * #remove this once we fixethe audio download issue
import tensorflow as tf
config = tf.ConfigProto()
session = tf.Session(config=config)
import scipy.signal as signal
import scipy
import librosa
from audio_helpers import *
from tqdm import tqdm
from mel_model_funcs import create_mel_model
from utils import mel_0_1


def getYelling(speaker_tagged, audio_file):

    #higher sampling rate makes it REALLY slow
    signal, sr = librosa.load(audio_file, sr = 100)
    second_indexes=np.linspace(0, len(signal)/sr, num=len(signal))
    indexed_signal = list(zip(second_indexes, signal))
    speaker_tagged['start_time'] = pd.to_numeric(speaker_tagged['start_time'])
    speaker_tagged['end_time'] = pd.to_numeric(speaker_tagged['end_time'])
    #this is a bit slow
    speaker_tagged['sentence_volume'] = speaker_tagged.apply(lambda row : np.mean([np.abs(i[1]) for i in indexed_signal if float(row['start_time']) <= i[0] < float(row['end_time'])] + [0]), axis = 1)
    #need to weight sentence volume by length
    speaker_tagged['weighted_volume'] = speaker_tagged.apply(lambda row: row['sentence_volume'] * (row['end_time'] - row['start_time']), axis = 1)
    speaker_average_volume = speaker_tagged.groupby('speaker').agg({'weighted_volume' : np.mean}).reset_index()
    speaker_lookup = dict(zip(speaker_average_volume['speaker'], speaker_average_volume['weighted_volume']))
    speaker_tagged['loudness_ratio'] = speaker_tagged.apply(lambda row : row['weighted_volume']/speaker_lookup[row['speaker']], axis =1)
    #we can empirically tweak this cuttoff once we have more videos
    yelling_cutoff = 1.5 #proportion of speakers average volume
    speaker_tagged['yelling'] = speaker_tagged.apply(lambda row : row['loudness_ratio']>yelling_cutoff, axis =1)

    return speaker_tagged[['speaker', 'start_time', 'end_time', 'speech', 'loudness_ratio', 'yelling']]


def getLaughter(audio_file_path, output_path, threshold = 0.9, min_length = 0.5):
    #TODO: We need to modify this function to return the time stamp of each incident of laughter in a dataframe like the other functions
    #Note that I adapted the input format to match that of yelling and that process audio is the driver file that calls both of them
    model_path = "./models/laughter_detector.h5"
    min_length = seconds_to_frames(min_length)
    laughs = segment_laughs(input_path = audio_file_path, model_path = model_path, output_path= output_path, threshold= threshold, min_length= min_length)
    print("found %d laughs." % (len(laughs)))
    ret = pd.DataFrame(laughs)

    if not ret.empty:
        ret['start_time'] = [float(x) for x in ret['start_time'].tolist()]
        ret['end_time'] = [float(x) for x in ret['end_time'].tolist()]
    return ret


def getApplause(audio_file_path, step=1):
    print("Loading applause model")
    model = create_mel_model()
    model.load_weights("./models/applause_detector.h5")

    print("Looking for applause")
    ret = segment_applause(input_path = audio_file_path, model = model, step = step)
    if not ret.empty:
        ret['start_time'] = [float(x) for x in ret['start_time'].tolist()]
        ret['end_time'] = [float(x) for x in ret['end_time'].tolist()]
    return ret


def process_audio(file_key, speaker_tagged, step=1):
    #TODO: see if there is a way to read directly rather than downloading to temp file
    if not os.path.exists('temp'):
        os.mkdir('temp')
    if not os.path.exists(os.path.join("temp", '{}.mp3'.format(file_key))):
        download_file_from_s3("classroom-bucket",
                              "audio/{}.mp3".format(file_key),
                              os.path.join("temp", '{}.mp3'.format(file_key)))
    yelling = getYelling(speaker_tagged, 'temp/{}.mp3'.format(file_key))
    laughter = getLaughter('temp/{}.mp3'.format(file_key), "temp")
    applause = getApplause('temp/{}.mp3'.format(file_key), step = step)
    #return yelling, laughter
    return yelling, laughter, applause
