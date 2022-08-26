#import insults
import configparser
import boto3
import json
import nltk
from nltk import sent_tokenize, download
nltk.download('vader_lexicon')
nltk.download('punkt')
nltk.download('words')
nltk.download('stopwords')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob
from insults import Insults
import pandas as pd
import numpy as np
import pickle
import re
from nltk import word_tokenize
import copy
from collections import Counter
import string
import re

from tagspeaker import *

# English word dictionary
from nltk.corpus import words
word_list = words.words()

from nltk.corpus import stopwords
stop = stopwords.words('english')


# jargon_list
jargon_list = pd.read_table('jargon-wl/word.lst', header=None)[0].tolist()
jargon_list = [j.lower() for j in jargon_list]
# Load stem module
from nltk.stem import PorterStemmer
ps = PorterStemmer()

# # Load Insulting pre-trained mode
Insults.load_model()


# # Load Sentiment model
vad = SentimentIntensityAnalyzer()

config = configparser.ConfigParser()
config.read('config.ini')
s3client = boto3.client(
    's3',
    aws_access_key_id=config['AWS']['ACCESS_KEY'],
    aws_secret_access_key=config['AWS']['SECRET_KEY']
)


def getTranscript(key):
    str_data = s3client.get_object(Bucket=config['AWS']['S3_BUCKET'],
                                   Key=key)['Body'].read()
    return json.loads(str_data)

def formatTranscript(transcript_json):
    """

    :param transcript_json: json file transcript downloaded from s3
    :return: a data.frame with
             1. Start time
             2. End time
             3. Sentence content
    """
    fullTranscript = transcript_json['results']['transcripts'][0]['transcript']

    # Convert result to a data.frame with variables of interest
    timedTranscript = transcript_json['results']['items']
    res = []
    for event in timedTranscript:
        temp = {}
        temp['start_time'] = event['start_time'] if 'start_time' in event.keys() else None
        temp['end_time'] = event['end_time'] if 'end_time' in event.keys() else None
        temp['type'] = event['type']
        temp['content'] = event['alternatives'][0]['content']
        temp['confidence'] = event['alternatives'][0]['confidence'] if event['alternatives'][0]['confidence'] is not None else float('NaN')
        res.append(temp)
    timedTranscript = pd.DataFrame.from_records(res, columns=['start_time','end_time','content','confidence','type'])
    timedTranscript.start_time = timedTranscript.start_time.astype(float)
    timedTranscript.end_time = timedTranscript.end_time.astype(float)
    return fullTranscript, timedTranscript





def getSentence(transcript, speaker_times):
    """
    Sentence is defined by stop punctuations and by speaker tags.
    :param transcript:
    :param tagged_speaker: data.frame
    :return: a DataFrame at sentence level
    """
    # Add speaker label to each word/punctuation
    speaker_times = [item for key,item in speaker_times.items() if item[1] is not None]
    intervals = [float(item[1]) for item in speaker_times]
    start_time_list = [0 if np.isnan(s) else s for s in transcript['start_time'].tolist()]
    start_time_index = [np.searchsorted(intervals,start_time,side="right") - 1 for start_time in start_time_list]
    speaker_tagged = []
    for i in range(len(start_time_index)):
        if start_time_index[i] == -1:
            if i == 0:
                speaker_tagged.append(None)
            else:
                speaker_tagged.append(speaker_times[start_time_index[i-1]][0])
        else:
            speaker_tagged.append(speaker_times[start_time_index[i]][0])
    transcript['speaker_label'] = speaker_tagged

    stop_punct = ['.','?','!']
    indexes_of_stop_punct = [i for i,x in enumerate(transcript['content']) if x in stop_punct]
    punct_index_list = [[e for e, punct_ind in enumerate(indexes_of_stop_punct) if punct_ind >= i] for i in transcript.index]
    transcript['punct_index'] = [l[0] if len(l) > 0 else len(indexes_of_stop_punct) for l in punct_index_list]
    # Create aggregated results
    sentence_index = []
    for i in transcript.index:
        if len(sentence_index) == 0:
            temp_index = 0
        else:
            temp_index = sentence_index[len(sentence_index)-1] + int(transcript['punct_index'][i] != transcript['punct_index'][i-1] or transcript['speaker_label'][i] != transcript['speaker_label'][i-1])
        sentence_index.append(temp_index)
    transcript['sentence_index'] = sentence_index

    start_time = transcript.groupby(['sentence_index','punct_index','speaker_label'],sort=False, as_index=False).apply(lambda x: x.start_time.min()).rename('start_time')
    end_time = transcript.groupby(['sentence_index', 'punct_index','speaker_label'], sort=False, as_index=False).apply(lambda x: x.end_time.max()).rename('end_time')
    sentence = transcript.groupby(['sentence_index', 'punct_index','speaker_label'], sort=False, as_index=False).apply(lambda x: x.content.str.cat(sep=' ')).rename('sentence')
    sentenceDF = pd.concat([start_time,
                            end_time,
                            sentence], axis=1)
    sentenceDF = sentenceDF.sort_values(by=['start_time']).reset_index().rename(columns={'index':'sentence_index'})
    return sentenceDF



def getSentencesForProcessing(sentenceDF):
    return sentenceDF['sentence'].tolist()

def getSentiment(sentences):
    """

    :param sentences:
    :return: data frame of sentence indexes and sentiment scores
    """

    # Get sentiment per sentence
    sent_score_blob = []
    sent_score_vader_neg = []
    sent_score_vader_neu = []
    sent_score_vader_pos = []
    for sentence in sentences:
        # TextBlob
        about_sentence = TextBlob(sentence)
        sentiment_score = about_sentence.sentiment.polarity
        sent_score_blob.append(sentiment_score)

        # NLTK Vader
        sent_score_vader_neg.append(vad.polarity_scores(sentence)['neg'])
        sent_score_vader_neu.append(vad.polarity_scores(sentence)['neu'])
        sent_score_vader_pos.append(vad.polarity_scores(sentence)['pos'])
    d = {'text_blob_score': sent_score_blob, 'vader_polarity_neg': sent_score_vader_neg, 'vader_polarity_neu': sent_score_vader_neu, 'vader_polarity_pos': sent_score_vader_pos}
    sentimentDF = pd.DataFrame(d).reset_index().rename(columns = {'index': 'sentence_index'})
    return sentimentDF


def getInsulting(sentences):
    """

    :param sentences:
    :return: data frame of sentence indexes, insultingness rating and list of foul language
    """
    insulting_rate = []
    foul_language = []
    for sentence in sentences:
        insulting_rate.append(Insults.rate_comment(sentence))
        foul_language.append(str(Insults.foul_language(sentence,context=False)[0]))

    d = {'insult_rating': insulting_rate, 'foul_language': foul_language}
    insultDF = pd.DataFrame(d).reset_index().rename(columns = {'index': 'sentence_index'})
    return insultDF

# Load Dialogue Act Classifier model
f = open('./models/dialogue_classifier.pickle', 'rb')
dialogue_classifier = pickle.load(f)
f.close()

def getDialogueType(sentences):
    res = []
    for sentence in sentences:
        res.append(dialogue_classifier.classify(dialogue_act_features(sentence)))

    d = {'sentence_type': res}
    dialogueDF = pd.DataFrame(d).reset_index().rename(columns = {'index': 'sentence_index'})
    return dialogueDF

# helper function for getDialogueType
def dialogue_act_features(post):
    features = {}
    for word in nltk.word_tokenize(post):
        features['contains({})'.format(word.lower())] = True
    return features


# helper function to extract words repeated more than threshold
def find_words_repeating(sentenceDF, threshold):
    sentences = sentenceDF['sentence'].tolist()
    res = []
    for sentence in sentences:
        sentence = re.sub(r'[^\w\s]', '', sentence)
        teacher_tokenize = word_tokenize(teacher)
        counts = Counter(teacher_tokenize)
        sentence_unique_words = list(np.unique(teacher_tokenize))
        res.append(dict((word, freq) for word, freq in counts.items() if freq >= threshold))


def get_word_counts(transcript, sentenceDF):
    start_time = sentenceDF['start_time'].iloc[0]
    end_time = sentenceDF['end_time'].iloc[-1]
    words = nltk.word_tokenize(transcript)
    unique_word_count = len(list(set(words)))
    word_count = len(words)
    wpm = word_count * 60 / (end_time - start_time)
    counted = Counter(words)
    atleast_2_times = [i[0] for i in counted.items() if i[1] >= 2]
    atleast_3_times = [i[0] for i in counted.items() if i[1] >= 3]
    atleast_5_times = [i[0] for i in counted.items() if i[1] >= 5]
    return [wpm, word_count, unique_word_count, atleast_2_times, atleast_3_times, atleast_5_times, sorted(words)]

def getJargon(transcript):
    transcript_words = nltk.word_tokenize(transcript)
    # remove stop words
    transcript_words = [w for w in transcript_words if w not in stop]
    # stem
    transcript_words_stem = [ps.stem(w) for w in transcript_words]

    # get word frequency
    counted = Counter(transcript_words)

    # get 'unofficial' words
    unofficial_words = set([w for i, w in enumerate(transcript_words) if w not in word_list and transcript_words_stem[i] not in word_list])
    #TODO: eventually do tf-idf scores for universe of classes in that subject once we have more training data
    jargon_words = set([w for i, w in enumerate(transcript_words) if w in jargon_list or transcript_words_stem[i] in jargon_list])
    unofficial_tf = {}
    jargon_tf = {}
    for j in unofficial_words:
        unofficial_tf[j] = counted[j]/len(transcript_words)

    for j in jargon_words:
        jargon_tf[j] = counted[j]/len(transcript_words)

    return jargon_tf, unofficial_tf

def cleanDocument(transcript):
    punc = string.punctuation
    table = str.maketrans('','',punc)
    transcript= transcript.translate(table)
    transcript = transcript.lower()
    return transcript

def getResponsePause(sentenceDF, dialogue_typeDF):
    """

    :param sentenceDF:
    :param dialogue_typeDF:
    :return: a dataframe with:
        + content
        + start time
        + end time
        + speaker_label
        + type
        + response duration between question and next response
        + same speaker or not
    """
    out = pd.concat([sentenceDF, dialogue_typeDF], axis=1)
    ret = []

    for i in out.index:
        if re.search('question', out['sentence_type'][i], re.IGNORECASE):
            temp = {
                "content": out['sentence'][i],
                "start_time": out['start_time'][i],
                "end_time": out['end_time'][i],
                "speaker_label": out['speaker_label'][i],
                "type": out['sentence_type'][i]                                         
            }
            if i == len(out.index)-1: # last sentence
                temp['response_duration'] = None
                temp['same_speaker'] = None
            else:
                temp['response_duration'] = out['start_time'][i+1] - out['end_time'][i]
                temp['same_speaker'] = out['speaker_label'][i+1] == out['speaker_label'][i]
            ret.append(temp)
    return pd.DataFrame(ret)







#driver to run all the natural language processing code
def process_nlp(file_key):
    transcript = getTranscript('transcription/{}.json'.format(file_key))
    full, timed = formatTranscript(transcript)
    speaker_times = getSpeakerTime(transcript)
    sentenceDF = getSentence(timed, speaker_times)
    sentences = getSentencesForProcessing(sentenceDF)
    sentimentDF = getSentiment(sentences)
    insultsDF = getInsulting(sentences)
    dialogue_typeDF = getDialogueType(sentences)
    words = get_word_counts(full, sentenceDF)
    jargon_df, unofficial_df = getJargon(cleanDocument(full))
    responsePause = getResponsePause(sentenceDF, dialogue_typeDF)

    return sentenceDF, sentimentDF, insultsDF, dialogue_typeDF, words, jargon_df, unofficial_df, responsePause




