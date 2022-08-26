import pickle
import nltk

f = open('./models/dialogue_act_classifier.pickle', 'rb')
dialogue_classifier = pickle.load(f)
f.close()

def dialogue_act_features(post):
     features = {}
     for word in nltk.word_tokenize(post):
         features['contains({})'.format(word.lower())] = True
     return features

