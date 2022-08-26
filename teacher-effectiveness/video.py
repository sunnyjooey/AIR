
"""
This script is to list all modules to extract features from a np array
"""

import face_recognition
from keras.models import load_model
import pandas as pd
import os
import video_helpers
from helpers import download_file_from_s3
import cv2


def getEmotion(inputPath):
    frame_number = 0
# initialize a list of pd.DataFrame
    output_ret = list()
    emotion_classifier = "./models/fer2013_mini_XCEPTION.102-0.66.hdf5"
    model_emotion = load_model(emotion_classifier, compile = False)
    input_movie = cv2.VideoCapture(inputPath)
    fps = input_movie.get(cv2.CAP_PROP_FPS)

    while True:
        ret, frame = input_movie.read()
        if not ret:  # if input cannot be read or if it is the last frame
            break
        frame_number += 1

        # Detect faces and match names
        face_locations = face_recognition.face_locations(frame)
        if len(face_locations) == 0:
            column_names = ["frame_number", "time","face_location"]
            frame_ret = [[frame_number, frame_number/fps, None]]
        else:
            frame_labels = [str(i) for i in range(len(face_locations))]
            # Initialize a nested list with each element is a row in the frame data.frame with the following columns:
            # frame_number
            # face_location
            column_names = ["frame_number", "time", "face_location"]
            frame_ret = [[frame_number, frame_number/fps, face_locations[i]] for i, x in enumerate(face_locations)]


            # Extract emotions
            emotions, emotions_prob = video_helpers.extract_emotion(frame, face_locations, model_emotion)
            column_names.append("emotion")
            column_names.append("emotion_prob")
            trash = [x.append(emotions[i]) for i, x in enumerate(frame_ret)]
            trash = [x.append(emotions_prob[i]) for i, x in enumerate(frame_ret)]

        # Append frame_ret to output_ret['csv']

        frame_ret = pd.DataFrame(frame_ret, columns=column_names)
        output_ret.append(frame_ret)

    input_movie.release()
    return pd.concat(output_ret)


def process_video(file_key, file_extension):
    if not os.path.exists('temp'):
        os.mkdir('temp')
    if not os.path.exists(os.path.join("temp", '{}.{}'.format(file_key, file_extension))):
        download_file_from_s3("classroom-bucket",
                              "video/{}.{}".format(file_key, file_extension),
                              os.path.join("temp", '{}.{}'.format(file_key, file_extension)))

    emotion = getEmotion('temp/{}.{}'.format(file_key, file_extension))
    return emotion
