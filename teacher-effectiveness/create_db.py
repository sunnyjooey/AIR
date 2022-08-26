#only need to run once to set up databases on RDS if created new install
import mysql.connector
import configparser
from sqlalchemy import create_engine
from helpers import connect

config = configparser.ConfigParser()
config.read('config.ini')

engine = connect()

#engine.execute('drop schema login;') #run this if you messed up the first time
engine.execute('create schema login;')
engine.execute('create table login.login (`username` varchar(32), `password` varchar(128), `login_attempts` int DEFAULT 0 NOT NULL, `teacher_id` int NOT NULL AUTO_INCREMENT, primary key (`teacher_id`))')

#engine.execute('drop schema features;') #run this if you messed up the first time
engine.execute('create schema features;')

engine.execute('create table features.file_locations (`username` varchar(32), `class_timestamp` datetime, `subject` varchar(248), `grade` varchar(248), `date` datetime, `video_location` varchar(248), `audio_location` varchar(248), `transcript_location` varchar(248));')

#these are all the tables with each row representing a sentence in the transcript
engine.execute('create table features.sentences(`username` varchar(32), `class_timestamp` datetime, `sentence_index` int, `punct_index` int, `speaker_label` text, `start_time` float, `end_time` float, `sentence` text)')
engine.execute('create table features.sentiment (`username` varchar(32), `class_timestamp` datetime, `sentence_index` int,  `text_blob_score` float, `vader_polarity_neg` float, `vader_polarity_neu` float, `vader_polarity_pos` float)')
engine.execute('create table features.insults (`username` varchar(32), `class_timestamp` datetime, `sentence_index` int, `insult_rating` float, `foul_language` float)')
engine.execute('create table features.questions (`username` varchar(32), `class_timestamp` datetime, `sentence_index` int, `sentence_type` varchar(128))')

#these are all the tables with each row a transcript
engine.execute('create table features.words (`username` varchar(32), `class_timestamp` datetime, `word_count` int, `unique_word_count` int, `atleast_2_times` int, `atleast_3_times` int, `atleast_5_times` int, `words` text)')
engine.execute('create table features.jargon (`username` varchar(32), `class_timestamp` datetime, `word` varchar(128), `tf` float)')

engine.execute('create table features.yelling(`username` varchar(32), `class_timestamp` datetime, `speaker` text, `start_time` float, `end_time` float, `speech` text, `loudness_ratio` float, `yelling` varchar(64))')
engine.execute('create table features.responsePauses(`username` varchar(32), `class_timestamp` datetime, `content` text, `end_time` float, `response_duration` float, `same_speaker` varchar(64), `speaker_label` varchar(64), `start_time` float, `type` varchar(64))')
engine.execute('create table features.speakertags(`username` varchar(32), `class_timestamp` datetime, `speaker` text,`start_time` float, `end_time` float, `speech` text)')

engine.execute('create table features.emotion(`username` varchar(32), `class_timestamp` datetime, `frame_number` int, `time` float, `face_location` text, `emotion` varchar(128), `emotion_prob` float)')

engine.execute('create table features.unofficial(`username` varchar(32), `class_timestamp` datetime, `word` varchar(128), `tf` float)')
