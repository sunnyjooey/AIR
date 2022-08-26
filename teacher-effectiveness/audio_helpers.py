import tensorflow as tf

import numpy as np
import scipy.signal as signal
import scipy
import librosa
import pandas as pd
from keras.models import load_model
import keras as kr
from utils import uni_len
from mel_model_funcs import TestGenerator
from tqdm import tqdm

config = tf.ConfigProto()
config.gpu_options.allow_growth = True
session = tf.Session(config=config)


def compute_mfcc_features(y, sr):
    mfcc_feat = librosa.feature.mfcc(y, sr, n_mfcc=12, n_mels=12, hop_length=int(sr / 100), n_fft=int(sr / 40)).T
    S, phase = librosa.magphase(librosa.stft(y, hop_length=int(sr / 100)))
    rms = librosa.feature.rms(S=S).T
    return np.hstack([mfcc_feat, rms])


def compute_delta_features(mfcc_feat):
    return np.vstack([librosa.feature.delta(mfcc_feat.T), librosa.feature.delta(mfcc_feat.T, order=2)]).T

def frame_to_time(frame_index):
    return (frame_index / 100.)


def seconds_to_frames(s):
    return (int(s * 100))


def collapse_to_start_and_end_frame(instance_list):
    return (instance_list[0], instance_list[-1])


def frame_span_to_time_span(frame_span):
    return (frame_span[0] / 100., frame_span[1] / 100.)


def seconds_to_samples(s, sr):
    return s * sr


def format_features(mfcc_feat, delta_feat, index, window_size=37):
    return np.append(mfcc_feat[index - window_size:index + window_size],
                     delta_feat[index - window_size:index + window_size])


def cut_laughter_segments(instance_list, y, sr):
    new_audio = []
    for start, end in instance_list:
        sample_start = int(seconds_to_samples(start, sr))
        sample_end = int(seconds_to_samples(end, sr))
        clip = y[sample_start:sample_end]
        new_audio = np.concatenate([new_audio, clip])
    return new_audio


def get_instances_from_rows(rows):
    return [(float(row.split(' ')[1]), float(row.split(' ')[2])) for row in rows]


def lowpass(sig, filter_order=2, cutoff=0.01):
    # Set up Butterworth filter
    filter_order = 2
    B, A = signal.butter(filter_order, cutoff, output='ba')

    # Apply the filter
    return (signal.filtfilt(B, A, sig))


def get_audio_instances(probs, threshold=0.5, min_length=0.2):
    instances = []
    current_list = []
    for i in range(len(probs)):
        if np.min(probs[i:i + 1]) > threshold:
            current_list.append(i)
        else:
            if len(current_list) > 0:
                instances.append(current_list)
                current_list = []
    instances = [frame_span_to_time_span(collapse_to_start_and_end_frame(i)) for i in instances if len(i) > min_length]
    return instances


def get_feature_list(y, sr, window_size=37):
    mfcc_feat = compute_mfcc_features(y, sr)
    delta_feat = compute_delta_features(mfcc_feat)
    zero_pad_mfcc = np.zeros((window_size, mfcc_feat.shape[1]))
    zero_pad_delta = np.zeros((window_size, delta_feat.shape[1]))
    padded_mfcc_feat = np.vstack([zero_pad_mfcc, mfcc_feat, zero_pad_mfcc])
    padded_delta_feat = np.vstack([zero_pad_delta, delta_feat, zero_pad_delta])
    feature_list = []
    for i in range(window_size, len(mfcc_feat) + window_size):
        feature_list.append(format_features(padded_mfcc_feat, padded_delta_feat, i, window_size))
    feature_list = np.array(feature_list)
    return feature_list


def format_outputs(instances, wav_paths):
    outs = []
    for i in range(len(instances)):
        outs.append({'filename': wav_paths[i], 'start_time': instances[i][0], 'end_time': instances[i][1]})
    return outs


def segment_laughs(input_path, model_path, output_path, threshold=0.5, min_length=0.2, write=False):
    print('Loading audio file...')

    y, sr = librosa.load(input_path, sr=8000)
    full_res_y, full_res_sr = librosa.load(input_path, sr=44100)

    print('Looking for laughter...')

    model = load_model(model_path)
    feature_list = get_feature_list(y, sr)

    probs = model.predict_proba(feature_list)
    probs = probs.reshape((len(probs),))  # .reshape((len(mfcc_feat),))
    filtered = lowpass(probs)
    instances = get_audio_instances(filtered, threshold=threshold, min_length=min_length)

    if len(instances) > 0:
        wav_paths = []
        maxv = np.iinfo(np.int16).max

        for index, instance in enumerate(instances):
            laughs = cut_laughter_segments([instance], full_res_y, full_res_sr)
            wav_path = output_path + "/laugh_" + str(index) + ".wav"
            # librosa.output.write_wav(wav_path, (laughs * maxv).astype(np.int16), full_res_sr)
            if write:
                scipy.io.wavfile.write(wav_path, full_res_sr, (laughs * maxv).astype(np.int16))
            wav_paths.append(wav_path)

        return (format_outputs(instances, wav_paths))

    else:
        return []




def get_feature_dict_for_applause(y, sr, window_size=37,step=1):
    melspec = librosa.feature.melspectrogram(y, sr=sr,
                                             n_fft =1764,
                                             hop_length = 220,
                                             n_mels=64)
    logmel = librosa.core.power_to_db(melspec)
    zero_pad_mfcc = np.zeros((logmel.shape[0], window_size))
    padded_mfcc_feat = np.hstack([zero_pad_mfcc, logmel, zero_pad_mfcc])

    feature_list = {}
    for i in range(window_size, logmel.shape[1] + window_size, step):
        feature_list[i] = padded_mfcc_feat[:, i - window_size:i + window_size]
    return feature_list

def segment_applause(input_path, model, window_size=37, step=1):
    y, sr = librosa.load(input_path, sr = None)
    y_trimmed = librosa.effects.trim(y)[0]

    feature_list = get_feature_dict_for_applause(y_trimmed, sr, window_size = window_size, step = step)

    prediction = np.log(np.ones((len(feature_list), 41)))

    labels = ['Acoustic_guitar', 'Applause', 'Bark', 'Bass_drum', 'Burping_or_eructation', 'Bus', 'Cello', 'Chime', 'Clarinet', 'Computer_keyboard', 'Cough', 'Cowbell', 'Double_bass', 'Drawer_open_or_close', 'Electric_piano', 'Fart', 'Finger_snapping', 'Fireworks', 'Flute', 'Glockenspiel', 'Gong', 'Gunshot_or_gunfire', 'Harmonica', 'Hi-hat', 'Keys_jangling', 'Knock', 'Laughter', 'Meow', 'Microwave_oven', 'Oboe', 'Saxophone', 'Scissors', 'Shatter', 'Snare_drum', 'Squeak', 'Tambourine', 'Tearing', 'Telephone', 'Trumpet', 'Violin_or_fiddle', 'Writing']
    for req_mel_len in [263, 363, 463, 563, 663, 763]:
        test_generator = TestGenerator(
            list(feature_list.keys()),
            mel_data = feature_list,
            req_mel_len = req_mel_len
        )

        this_pred = model.predict_generator(
            test_generator,
            steps = len(test_generator),
            max_queue_size = 1,
            workers = 1,
            use_multiprocessing = False)

        prediction = prediction + np.log(this_pred + 1e-38)
        del test_generator, this_pred

    feature_list = [item for key, item in feature_list.items()]
    predicted_labels = np.array(labels)[np.argsort(-prediction, axis = 1)[:, :3]]
    predicted_labels = ['Applause' in x for x in predicted_labels]

    instances = []
    for i in range(len(feature_list)):
        if predicted_labels[i]:
            instances.append(i)

    seconds_segments = [frame_span_to_time_span([i, i+1]) for i in instances]

    ret = pd.DataFrame(seconds_segments, columns = ['start_time','end_time'])
    return ret
