import configparser
import functools
import json
import os
import re

import numpy as np
import pandas as pd
import plotly
from flask import Flask, abort, flash, redirect, render_template, request, session, url_for, g

# must use named imports rahter htan import * from audio because otherwise Session overrides Flask's session leading to
# errors like "session has no attribue get()" when you try to login
# changing the name of the Session variable in audio does not resolve this
from werkzeug.exceptions import HTTPException, InternalServerError
from werkzeug.security import check_password_hash

from audio import process_audio
from config import ACCOUNT_LOCKOUT_THRESHOLD, PORT, DEBUG
from helpers import QueryFile, datetime, get_username_list, add_user, get_user_data, update_failed_login, allowed_file, \
    check_file_format, upload_file_to_s3, download_file_from_s3, convert_to_audio, transcribe_audio, \
    file_locations_to_db, connect, results_to_db, extractFeatureTable, extractClassInfo, dayNameFromWeekday

from nlp import getTranscript, process_nlp
from tagspeaker import getSpeakerTime, tagSpeakers
from video import process_video
from visualize import plot_sentiment, get_words_for_cloud, get_word_count, plot_speed, plot_jargon, plot_question, \
    plot_proportions, plot_time, plot_emotion, plot_occurrence, get_data_for_occurrence, pause_analysis

config = configparser.ConfigParser()
config.read('config.ini')
app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.environ.get("SECRET_KEY", default="secret")
app.session_cookie_name = os.environ.get("SESSION_COOKIE_NAME", default="session")
app.config.env = app.env
app.queryFile = QueryFile()


def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            flash("Authentication is required to visit this page.")
            return redirect(url_for("login"))
        return view(**kwargs)

    return wrapped_view


# Authentication
@app.route("/", methods=["GET"])
@login_required
def main():
    return render_template('index.html')


@app.route("/register", methods=["GET", "POST"])
def register():
    if app.env == 'public':
        abort(403)
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        error = None

        if not username:
            error = 'Username is required.'
        if not password:
            error = 'Password is required.'

        registered_list = get_username_list()
        if username in registered_list:
            error = 'User {} is already registered.'.format(username)
        else:
            add_user(username, password)
        if error is None:
            return redirect(url_for('login'))

        flash(error)
    return render_template('register.html')


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        error = None
        registered_list = get_username_list()

        if username in registered_list:
            user_data = get_user_data(username)
            if user_data['login_attempts'] < ACCOUNT_LOCKOUT_THRESHOLD:
                if check_password_hash(user_data['password'], str(password)):
                    update_failed_login(username, reset=True)
                else:
                    update_failed_login(username)
                    error = 'Incorrect password.'
            else:
                update_failed_login(username)
                error = 'Account is locked out. Please contact AIR.'
        else:
            error = 'Username does not exist. Please register first.'

        if error is None:
            session.clear()
            session['user_id'] = username
            return redirect(url_for('main'))

        flash(error)
    return render_template('login.html')


@app.before_request
def load_logged_in_user():
    g.user = session.get('user_id')


@app.route('/logout', methods=["GET"])
def logout():
    session.clear()
    return redirect(url_for('login'))


# Upload a file and fill in the form
@app.route("/upload", methods=["GET"])
@login_required
def upload():
    if app.env == 'public':
        abort(403)
    if request.method == "GET":
        return render_template('upload.html')


@app.route("/uploaded", methods=["POST"])
@login_required
def uploaded():
    if app.env == 'public':
        abort(403)
    if request.method == "POST":
        file = request.files['user_file']
        if file.filename == "":
            return "Please select a file"
            # file locations table

        if file and allowed_file(file.filename):
            # Step 1. Check the file extension
            file_type = check_file_format(file.filename)
            file_extension = file.filename.split('.')[-1].lower()
            file_key = os.path.splitext(file.filename)[0]
            # cleaning file_key
            file_key = re.sub(r'[?|$|!]', r'', file_key)  # remove some special characters for transcription job
            file_key = re.sub(r'[\(|\)| |\[|\]]', '_', file_key)  # replace space and brackets with underscore
            file_key = re.sub('_$', '', file_key)
            file_name = "{}.{}".format(file_key, file_extension)

            # Initiate a data frame to store form input
            result = request.form
            result = result.to_dict(flat=True)
            result['date'] = datetime.strptime(result['date'], '%m/%d/%Y')
            result['time_stamp'] = datetime.now()

            if file_type == "video":
                # Save video file to S3
                output = upload_file_to_s3(file, "classroom-bucket", "video/{}".format(file_name))
                # Save it to a temp file
                if not os.path.exists('temp'):
                    os.mkdir('temp')
                download_file_from_s3("classroom-bucket", "video/{}".format(file_name), os.path.join("temp", file_name))

                # Convert video temp file to an audio file and upload to S3
                audio_output = convert_to_audio(os.path.join("temp", file_name), file_key)
                transcript_output = transcribe_audio(audio_output, speakers=2, key=file_key + ".json")

                # Save result
                result['Video Location'] = str(output)
                result['Audio Location'] = str(audio_output)
                result['Transcript Location'] = str(transcript_output)
            elif file_type == "audio":
                output = upload_file_to_s3(file, "classroom-bucket", "audio/{}".format(file_name))
                transcript_output = transcribe_audio(output, speakers=2, key=file_key + ".json")

                result['Video Location'] = ""
                result['Audio Location'] = str(output)
                result['Transcript Location'] = str(transcript_output)

            file_locations_to_db(g.user, result)
            # nlp data
            speaker_times = getSpeakerTime(getTranscript("transcription/{}.json".format(file_key)))
            speaker_tagged = tagSpeakers(getTranscript("transcription/{}.json".format(file_key)), speaker_times)
            sentenceDF, sentimentDF, insultsDF, dialogue_typeDF, words, jargon_tf, unofficial_tf, responsePauseDF = process_nlp(
                file_key)

            # audio data
            # laughter is currently empty
            yellingDF, laughterDF, applauseDF = process_audio(file_key, speaker_tagged, step=20)
            if len(applauseDF) == 0:
                applauseDF = pd.DataFrame([[np.nan, np.nan]], columns=['start_time', 'end_time'])

            engine = connect()

            # process jargon and unofficial dict
            if len(jargon_tf) > 0:
                jargon_df = pd.DataFrame([[i, t] for i, t in jargon_tf.items()], columns=['word', 'tf'])
            else:
                jargon_df = pd.DataFrame([['', 0.0]], columns=['word', 'tf'])

            if len(unofficial_tf) > 0:
                unofficial_df = pd.DataFrame([[i, t] for i, t in unofficial_tf.items()], columns=['word', 'tf'])
            else:
                unofficial_df = pd.DataFrame([['', 0.0]], columns=['word', 'tf'])

            df_dict = {'sentiment': sentimentDF, 'insults': insultsDF, 'question': dialogue_typeDF,
                       'sentences': sentenceDF, 'responsePauses': responsePauseDF,
                       'yelling': yellingDF, 'laughter': laughterDF, 'applause': applauseDF,
                       'jargon': jargon_df, 'unofficial': unofficial_df}

            # video data
            if file_type == "video":
                emotionDF = process_video(file_key, file_extension)
                values = {'emotion': 'Unknown', 'emotion_prob': 0.0, 'face_location': 'Unknown'}
                emotionDF.fillna(value=values, inplace=True)
                emotionDF['emotion_prob'] = emotionDF['emotion_prob'].astype(float)
                emotionDF['face_location'] = emotionDF['face_location'].astype(str)
                df_dict['emotion'] = emotionDF

            for table, df in df_dict.items():
                if not df.empty:
                    results_to_db(df, table, engine, 'features', g.user, result['time_stamp'])
                    print("{} pushed".format(table))
                else:
                    print("{} skipped".format(table))

            app.queryFile = QueryFile()

            return render_template("uploaded.html", result=result)
        else:
            error = "File format is not allowed. Please upload only audio and video file."
            flash(error)
            return redirect("/")


# Query a File for Analysis
@app.route("/query", methods=["GET", "POST"])
@login_required
def query():
    subject_list = app.queryFile.get_unique_list("subject")
    grade_list = app.queryFile.get_unique_list("grade")
    weekday_list = app.queryFile.get_unique_list("weekday")
    show_results = False
    ret = {}

    if request.method == "POST":
        subject = request.form.get('subject')
        grade = request.form.get('grade')
        weekday = request.form.getlist('weekday')
        show_results = True

        if (subject in subject_list + ["Select One Subject"]) \
                and (grade in grade_list + ["Select One Grade"]) \
                and all(w in weekday_list for w in weekday):
            ret = app.queryFile.show_query_results(subject=subject, grade=grade, weekday=weekday)
            ret = ret.to_dict('index')
        else:
            abort(404)

    return render_template('query.html',
                           subject_list=subject_list,
                           grade_list=grade_list,
                           weekday_list=weekday_list,
                           showResults=show_results,
                           results=ret)


@app.route("/p/<file_key>/analysis", methods=["GET"])
@login_required
def analysis(file_key):
    if file_key not in app.queryFile.file_key_df['MD5'].values:
        abort(404)
    return render_template('analysis.html', file_key=file_key)


@app.route("/p/<file_key>/word_cloud", methods=["GET"])
@login_required
def word_cloud(file_key):
    if file_key not in app.queryFile.file_key_df['MD5'].values:
        abort(404)
    username, class_timestamp = app.queryFile.get_pair_key(file_key)
    sentenceDF = extractFeatureTable(username, class_timestamp, "sentences")
    teacher_wr_weight = get_words_for_cloud(sentenceDF)
    return teacher_wr_weight


@app.route("/p/<file_key>/analysis_emotion", methods=["GET"])
@login_required
def analysis_emotion(file_key):
    if file_key not in app.queryFile.file_key_df['MD5'].values:
        abort(404)
    username, class_timestamp = app.queryFile.get_pair_key(file_key)
    sentimentDF = extractFeatureTable(username, class_timestamp, "sentiment")
    sentenceDF = extractFeatureTable(username, class_timestamp, "sentences")

    # 1. sentiment line graph
    figure = plot_sentiment(sentimentDF, sentenceDF)
    graphJSON_sentiment = json.dumps(figure, cls=plotly.utils.PlotlyJSONEncoder)

    # 2. emotion heat map
    emotionDF = extractFeatureTable(username, class_timestamp, "emotion")
    figure = plot_emotion(emotionDF)
    graphJSON_emotion = json.dumps(figure, cls=plotly.utils.PlotlyJSONEncoder)

    # 3. text box for number of occurrences
    insultDF = extractFeatureTable(username, class_timestamp, "insults")
    yelling = extractFeatureTable(username, class_timestamp, "yelling")
    laughter = extractFeatureTable(username, class_timestamp, "laughter")
    applause = extractFeatureTable(username, class_timestamp, "applause")
    insult, laugh, yelling, applause = get_data_for_occurrence(sentenceDF, insultDF, laughter, yelling, applause)
    occurrence_data = {
        "insult": len(insult),
        "laugh": len(laugh),
        "yelling": len(yelling),
        "applause": len(applause)
    }

    # 4. stack chart for timeline of occurrences
    figure = plot_occurrence(insult, laugh, yelling, applause)
    graphJSON_occurrence = json.dumps(figure, cls=plotly.utils.PlotlyJSONEncoder)

    # 5. class info
    classinfoDF = extractClassInfo(username, class_timestamp)
    class_info = {
        'Teacher': username,
        'Grade': classinfoDF.loc[0, 'grade'],
        'Subject': classinfoDF.loc[0, 'subject'],
        'Date': dayNameFromWeekday(classinfoDF.loc[0, 'date'].weekday()) + ', ' + str(
            classinfoDF.loc[0, 'date'].strftime('%B %d, %Y'))
    }

    return render_template('analysis_emotion.html', file_key=file_key,
                           class_info=class_info,
                           graphJSON_sentiment=graphJSON_sentiment,
                           graphJSON_emotion=graphJSON_emotion,
                           graphJSON_occurrence=graphJSON_occurrence,
                           occurrence_data=occurrence_data)


@app.route("/p/<file_key>/analysis_organization", methods=["GET"])
@login_required
def analysis_organization(file_key):
    if file_key not in app.queryFile.file_key_df['MD5'].values:
        abort(404)
    username, class_timestamp = app.queryFile.get_pair_key(file_key)
    responsePause = extractFeatureTable(username, class_timestamp, "responsePauses")
    sentenceDF = extractFeatureTable(username, class_timestamp, "sentences")

    # Get speaker proportions
    figure_prop, who_is_teacher = plot_proportions(sentenceDF, responsePause)
    graphJSON_prop = json.dumps(figure_prop, cls=plotly.utils.PlotlyJSONEncoder)

    # Get speaker timeline
    figure_time = plot_time(sentenceDF, responsePause, who_is_teacher)
    graphJSON_time = json.dumps(figure_time, cls=plotly.utils.PlotlyJSONEncoder)

    # Get pause pivot table
    pause_data = pause_analysis(sentenceDF, responsePause)

    # Class info
    classinfoDF = extractClassInfo(username, class_timestamp)
    class_info = {
        'Teacher': username,
        'Grade': classinfoDF.loc[0, 'grade'],
        'Subject': classinfoDF.loc[0, 'subject'],
        'Date': dayNameFromWeekday(classinfoDF.loc[0, 'date'].weekday()) + ', ' + str(
            classinfoDF.loc[0, 'date'].strftime('%B %d, %Y'))
    }

    return render_template('analysis_organization.html', file_key=file_key,
                           class_info=class_info,
                           graphJSON_prop=graphJSON_prop,
                           graphJSON_time=graphJSON_time,
                           pause_data=pause_data)


@app.route("/p/<file_key>/analysis_instruction", methods=["GET"])
@login_required
def analysis_instruction(file_key):
    if file_key not in app.queryFile.file_key_df['MD5'].values:
        abort(404)
    # Get number of unique words
    username, class_timestamp = app.queryFile.get_pair_key(file_key)
    sentenceDF = extractFeatureTable(username, class_timestamp, "sentences")
    num_words = get_word_count(sentenceDF)

    # Get words per min
    figure_speed, word_speed = plot_speed(sentenceDF)
    graphJSON_speed = json.dumps(figure_speed, cls=plotly.utils.PlotlyJSONEncoder)

    # Get jargon words
    jargonDF = extractFeatureTable(username, class_timestamp, "jargon")
    figure_jargon = plot_jargon(jargonDF, sentenceDF)
    graphJSON_jargon = json.dumps(figure_jargon, cls=plotly.utils.PlotlyJSONEncoder)

    # Get questions
    dialogueDF = extractFeatureTable(username, class_timestamp, "question")
    figure_question, question_count = plot_question(dialogueDF, sentenceDF)
    graphJSON_question = json.dumps(figure_question, cls=plotly.utils.PlotlyJSONEncoder)

    # Class info
    classinfoDF = extractClassInfo(username, class_timestamp)
    class_info = {
        'Teacher': username,
        'Grade': classinfoDF.loc[0, 'grade'],
        'Subject': classinfoDF.loc[0, 'subject'],
        'Date': dayNameFromWeekday(classinfoDF.loc[0, 'date'].weekday()) + ', ' + str(
            classinfoDF.loc[0, 'date'].strftime('%B %d, %Y'))
    }

    return render_template("analysis_instruction.html", file_key=file_key,
                           class_info=class_info,
                           word_count=num_words,
                           graphJSON_speed=graphJSON_speed,
                           graphJSON_jargon=graphJSON_jargon,
                           graphJSON_question=graphJSON_question,
                           word_speed=word_speed,
                           question_count=question_count)


@app.errorhandler(Exception)
def error_handler(e):
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return render_template('error.html', error=e), e.code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=DEBUG)
