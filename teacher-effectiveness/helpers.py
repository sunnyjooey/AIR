import calendar
import configparser
import datetime
import hashlib
import json
import os.path
import time
import urllib
from datetime import datetime

import boto3
import pandas as pd
from moviepy.editor import *
from sqlalchemy import create_engine, text
from werkzeug.security import generate_password_hash

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Read in AWS crendential information
config = configparser.ConfigParser()
config.read('config.ini')
s3client = boto3.client(
    's3',
    aws_access_key_id=config['AWS']['ACCESS_KEY'],
    aws_secret_access_key=config['AWS']['SECRET_KEY']
)
transcribe = boto3.client(
    'transcribe',
    aws_access_key_id=config['AWS']['ACCESS_KEY'],
    aws_secret_access_key=config['AWS']['SECRET_KEY'],
    region_name=config['AWS']['REGION_NAME']
)

S3_BUCKET = config['AWS']['S3_BUCKET']
S3_LOCATION = 'http://{}.s3.amazonaws.com/'.format(S3_BUCKET)


def upload_file_to_s3(file, bucket_name, key, acl="public-read"):
    try:

        s3client.upload_fileobj(
            file,
            bucket_name,
            key,
            ExtraArgs={
                "ACL": acl,
                "ContentType": file.content_type
            }
        )

    except Exception as e:
        # This is a catch all exception, edit this part to fit your needs.
        print("Something Happened: ", e)
        return e
    return "{}{}".format(S3_LOCATION, key)


ALLOWED_EXTENSIONS = set(['mov', 'mp3', 'mp4', 'avi', 'wav'])


def allowed_file(filename):
    return '.' in filename and \
           filename.split('.')[-1].lower() in ALLOWED_EXTENSIONS


def check_file_format(filename):
    file_extension = filename.split('.')[-1].lower()
    if file_extension in ['wav', 'mp3']:
        return "audio"
    elif file_extension in ['mov', 'avi', 'mp4']:
        return "video"
    else:
        return "other"


def convert_to_audio(filepath, file_key):
    audioclip = AudioFileClip(filepath)
    filename = os.path.basename
    if not os.path.exists('temp'):
        os.mkdir('temp')
    audioclip.write_audiofile('temp/' + file_key + '.mp3')
    print("finishing writing to mp3")
    s3client.upload_file('temp/' + file_key + '.mp3',
                         S3_BUCKET,
                         "audio/" + file_key + ".mp3")
    return "{}{}".format(S3_LOCATION, "audio/" + file_key + ".mp3")


## transcribe audio function
def transcribe_audio(file_uri, speakers, key):
    sts = {"status": 0}
    try:
        audio_format = file_uri.split(".")[-1].lower()
        transcribe_name = "{}_transcription_{}".format(os.path.basename(file_uri), speakers)
        transcribe_jobs = transcribe.list_transcription_jobs()["TranscriptionJobSummaries"]
        settings = {"ShowSpeakerLabels": speakers > 1}
        if settings["ShowSpeakerLabels"]:
            settings["MaxSpeakerLabels"] = speakers
        if len([t for t in transcribe_jobs if t["TranscriptionJobName"] == transcribe_name]) == 0:
            transcribe.start_transcription_job(TranscriptionJobName=transcribe_name,
                                               Media={"MediaFileUri": file_uri},
                                               MediaFormat=audio_format,
                                               LanguageCode="en-US",
                                               Settings=settings)
            print("Transcription job '{}' was submitted".format(transcribe_name))
        else:
            print("Transcription job '{}' already exists".format(transcribe_name))
        while True:
            transcription_job = transcribe.get_transcription_job(TranscriptionJobName=transcribe_name)
            start_time = transcription_job["TranscriptionJob"].get("CreationTime").replace(tzinfo=None)
            finish_time = transcription_job["TranscriptionJob"].get("CompletionTime")
            finish_time = finish_time.replace(tzinfo=None) if finish_time else datetime.now().replace(tzinfo=None)
            status = transcription_job["TranscriptionJob"]["TranscriptionJobStatus"]
            print("Transcription job '{}' is {}, {:.0f}s".format(transcribe_name, status,
                                                                 (finish_time - start_time).total_seconds()))
            if status in ["COMPLETED", "FAILED"]:
                break
            time.sleep(5)
        sts["transcription"] = json.loads(urllib.request.urlopen(
            transcription_job["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]).read())
        s3client.put_object(Body=json.dumps(sts['transcription']),
                            Bucket=S3_BUCKET,
                            Key="transcription/{}".format(key))
    except Exception as e:
        sts["status"] = 1
        print(str(e))
        return ""
    return "{}{}".format(S3_LOCATION, "transcription/{}".format(key))


## utils function
def download_file_from_s3(bucket_name,
                          key,
                          savepath):
    s3client.download_file(bucket_name,
                           key,
                           savepath)


def get_s3_keys(bucket):
    """Get a list of keys in an S3 bucket."""
    keys = []
    resp = s3client.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    return keys


def connect():
    engine = create_engine(
        'mysql+pymysql://{}:{}@{}'.format(config['AWS']['RDS_USERNAME'], config['AWS']['RDS_PASSWORD'],
                                          config['AWS']['RDS_HOST']))
    return engine


def get_username_list():
    db = connect()
    usernames = db.execute("select username from login.login")
    registered_list = [item['username'] for item in usernames]
    return registered_list


def get_user_data(username):
    db = connect()
    user_row = db.execute(text("select password, login_attempts from login.login where username = :username"),
                          {"username": username})
    user_data = [{'password': item['password'], 'login_attempts': item['login_attempts']} for item in user_row][0]
    return user_data


def update_failed_login(username, reset=False):
    db = connect()
    db.execute(text(
        "update login.login set login_attempts = case when :reset then 0 else login_attempts+1 end where username = :username"),
        {"username": username, "reset": reset})


def add_user(username, password):
    db = connect()
    db.execute("insert into login.login (`username`, `password`) values ('{}', '{}')".format(username,
                                                                                             generate_password_hash(
                                                                                                 str(password))))


def file_locations_to_db(user, result):
    '''
    :params user: usernmae
            result: dataframe with form info from upload form
    :returns: None, commits results to database
    '''
    # Save result to a data.frame
    db = connect()
    # this has temp values for testing
    db.execute('''insert into features.file_locations (`username`, `class_timestamp`, `subject`, `grade`, `date`, `video_location`, `audio_location`, `transcript_location`) 
        values ('{}', '{}', '{}', '{}','{}', '{}', '{}', '{}')'''.format(user, result['time_stamp'], result['subject'],
                                                                         result['grade'], result['date'],
                                                                         result['Video Location'],
                                                                         result['Audio Location'],
                                                                         result['Transcript Location']))


def results_to_db(resultsDF, upload_table, engine, db_name, username, class_timestamp):
    existing_cols = list(resultsDF.columns)
    resultsDF['username'] = username
    resultsDF['class_timestamp'] = class_timestamp
    # change the order so the username and class_timestamp were first
    resultsDF = resultsDF[['username', 'class_timestamp'] + existing_cols]
    resultsDF.to_sql(con=engine, schema=db_name, name=upload_table, index=False, if_exists='append')


def extractFeatureTable(username, class_timestamp, table_name):
    db = connect()
    db.execute('use features')
    return (pd.read_sql(
        '''select * from {} where `username` = '{}' and `class_timestamp` = '{}' '''.format(table_name, username,
                                                                                            class_timestamp), con=db))


def extractClassInfo(username, class_timestamp):
    db = connect()
    db.execute('use features')
    return (pd.read_sql(
        '''select subject, grade, date from file_locations where `username` = '{}' and `class_timestamp` = '{}' '''.format(
            username, class_timestamp), con=db))


def filterTable(subject, grade):
    db = connect()
    db.execute('use features')
    ret = pd.read_sql(
        '''select username, class_timestamp from file_locations where `subject` = '{}' and `grade` = '{}' '''.format(
            subject, grade), con=db)
    return ret


def dayNameFromWeekday(weekday):
    if weekday == 0:
        return "Monday"
    if weekday == 1:
        return "Tuesday"
    if weekday == 2:
        return "Wednesday"
    if weekday == 3:
        return "Thursday"
    if weekday == 4:
        return "Friday"
    if weekday == 5:
        return "Saturday"
    if weekday == 6:
        return "Sunday"


# Helper class to set
class QueryFile:
    def __init__(self):
        db = connect()
        self.file_key_df = pd.read_sql('select * from features.file_locations', con=db)
        # Get weekday
        self.file_key_df['weekday'] = self.file_key_df['date'].apply(lambda x: calendar.day_name[x.weekday()])
        # Get hashkey
        self.file_key_df['MD5'] = [hashlib.md5(
            (self.file_key_df['username'][i] + str(self.file_key_df['class_timestamp'][i])).encode()).hexdigest() for i
                                   in self.file_key_df.index]

    def get_unique_list(self, column_name):
        return sorted(self.file_key_df[column_name].unique(),
                      key=lambda x:
                      list(calendar.day_name).index(x) if column_name == "weekday"
                      else (int(x) if x.isdigit() else float('inf'), str(x).lower()) if column_name == "grade"
                      else str(x).lower())

    def show_query_results(self, subject, grade, weekday):
        return self.file_key_df[
            self.file_key_df['subject'].isin([subject]) & self.file_key_df['grade'].isin([grade]) & self.file_key_df[
                'weekday'].isin(weekday)]

    def get_pair_key(self, MD5):
        ret = self.file_key_df[self.file_key_df['MD5'] == MD5]
        return ret['username'].values[0], ret['class_timestamp'].values[0]
