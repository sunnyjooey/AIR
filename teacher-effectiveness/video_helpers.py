"""
video_helpers.py contains the script to pre-train the model(s) (because we only need to train the model once).
"""

from keras.models import load_model
import numpy as np
import cv2
import numpy as np



def preprocess_input(x, v2=True):
    x = x.astype('float32')
    x = x / 255.0
    if v2:
        x = x - 0.5
        x = x * 2.0
    return x

def apply_offsets(face_locations, offsets):
    top, right, bottom, left = face_locations
    x = left
    y = top
    width = right - left
    height = bottom - top
    x_off, y_off = offsets
    return (x - x_off, x + width + x_off, y - y_off, y + height + y_off)


def extract_emotion(frame, face_locations, emotion_classifier):
    """
    :param frame
    :param face_location: a tuple (x,y,z,t) to crop the face out of a frame
    :param emotion_classifier: a trained model to classify 7 emotions
           0=Angry, 1=Disgust, 2=Fear, 3=Happy, 4=Sad, 5=Surprise, 6=Neutral
    :param debugMode: boolean to indicate whether we should print out a photo
           with the emotion label for debugging purpose
    :return: a tuple of emotion names and probability
    """
    emotion_labels = {0:'angry',1:'disgust',2:'fear',3:'happy',
                4:'sad',5:'surprise',6:'neutral'}


    # hyper-parameters for bounding boxes shape
    emotion_offsets = (20, 40)

    # getting input model shapes for inference
    emotion_target_size = emotion_classifier.input_shape[1:3]
    emotion_probability = []
    emotion_text = []
    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    for face_location in face_locations:
        x1, x2, y1, y2 = apply_offsets(face_location, emotion_offsets)
        gray_face = frame[y1:y2, x1:x2] #gray_image[y1:y2, x1:x2]

        try:
            gray_face = cv2.resize(gray_face, (emotion_target_size))
        except:
            emotion_probability.append(0.0)
            emotion_text.append(None)
            continue

        gray_face = preprocess_input(gray_face, True)
        gray_face = np.expand_dims(gray_face, 0)
        gray_face = np.expand_dims(gray_face, -1)
        emotion_prediction = emotion_classifier.predict(gray_face)
        emotion_probability.append(np.max(emotion_prediction))
        emotion_label_arg = np.argmax(emotion_prediction)
        emotion_text.append(emotion_labels[emotion_label_arg])

    return emotion_text, emotion_probability