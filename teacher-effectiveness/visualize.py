import json
import plotly.graph_objs as go
import matplotlib.pyplot as plt
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import pandas as pd
import numpy as np
from collections import defaultdict
import math


def plot_sentiment(sentimentDF, sentenceDF):
    """
    :param sentimentDF, sentenceDF: pd df returned from process_nlp
    :return: dict ready for visualization
    """
    sentiment_sentence = pd.merge(sentenceDF, sentimentDF, how='left', on='sentence_index', suffixes=('', '_x'))
    sentiment = sentiment_sentence['text_blob_score']
    max_minute = sentiment_sentence.loc[sentiment_sentence.shape[0]-1, 'end_time'] /60
    round_five = int(math.ceil(max_minute / 5)) * 5

    # Create plot
    trace_sentiment = go.Scatter(
        x=[i for i in list(sentiment_sentence['end_time'])],
        y=sentiment,
        mode='lines+markers',
        name='Sentiment'
    )
    data = [trace_sentiment]

    # Designate layout
    layout = go.Layout(
        title='Sentiment: Positive or Negative Over Time',
        titlefont=dict(
            size=26),
        yaxis=dict(
            title='Sentiment',
            titlefont=dict(
                size=18),
            range=[-1.25, 1.25]
        ),
        xaxis=dict(
            titlefont=dict(
                size=18),
            tickvals=np.arange(0, (round_five+1)*60, (round_five*60)/5),
            ticktext=np.arange(0, (round_five+1), round_five/5),
            title='Time (minutes)'
        ),
        font=dict(family='serif')
    )

    figure = dict(data=data, layout=layout)
    return figure


def get_teacher(sentenceDF):
    """
    Helper function to decide who is the teacher
    The longer string is the teacher
    :param sentenceDF: pd df returned from process_nlp
    :return: list of teacher's words, speaker tag of teacher
    """
    spkDF1 = sentenceDF[sentenceDF['speaker_label'] == 'spk_1']
    spkDF0 = sentenceDF[sentenceDF['speaker_label'] == 'spk_0']
    spkStr1 = spkDF1['sentence'].str.cat(sep=' ')
    spkStr0 = spkDF0['sentence'].str.cat(sep=' ')

    if len(spkStr0) > len(spkStr1):
        teacherStr = spkStr0
        teacher = 'spk_0'
    else:
        teacherStr = spkStr1
        teacher = 'spk_1'
    teacherStr = teacherStr.lower()
    teacher_words = word_tokenize(teacherStr)
    return teacher_words, teacher


def get_words_for_cloud(sentenceDF):
    """
    Processes sentences for word cloud
    :param sentenceDF: pd df returned from process_nlp
    :return: json of word counts
    """
    teacher_words, _ = get_teacher(sentenceDF)
    stop_words = set(stopwords.words('english'))

    # Remove stopwords from our words list
    words_to_vis = [word for word in teacher_words if word not in stop_words and word.isalpha()]

    # Words to visualize (only frequent ones) and count (all)
    words_freq_vis = Counter(words_to_vis)
    words_json_vis = [{'text': word, 'weight': count} for word, count in words_freq_vis.items()]
    return json.dumps(words_json_vis)


def get_word_count(sentenceDF):
    """
    Processes sentences to count unique words
    :param sentenceDF: pd df returned from process_nlp
    :return: integer of number of unique words used
    """
    teacher_words, _ = get_teacher(sentenceDF)
    words_to_count = Counter(teacher_words)
    return len(words_to_count.items())


def plot_speed(sentenceDF):
    """
    Get words-per-min of teacher's speech
    and plot against average speed
    :param sentenceDF: pd df returned from process_nlp
    :return: dict ready for visualization
    """
    # Calculate words per min
    teacher_words, teacher = get_teacher(sentenceDF)
    teacherDF = sentenceDF[sentenceDF['speaker_label'] == teacher]
    teacherDF['spk_time'] = teacherDF['end_time'] - teacherDF['start_time']
    len_in_min = teacherDF['spk_time'].sum() / 60
    teacher_words_spoken_per_min = len(teacher_words) / round(len_in_min)

    # Draw graph
    trace_ = go.Bar(
        x=[250],
        y=[0],
        name='All teachers',
        text='All teachers',
        orientation='h'
    )

    layout_1 = go.Layout(
        barmode='stack',
        title='Words Spoken per Minute',
        titlefont=dict(
            size=26),
        font=dict(
            family='serif'),
        xaxis=dict(
            range=[0, 250],
            showline=True,
            showgrid=False,
            title='Number of Words',
            titlefont=dict(
                size=18),
        ),
        yaxis=dict(
            range=[-1, 1],
            showgrid=False,
            showline=False,
            showticklabels=False,
        ),

        annotations=[
            dict(
                x=int(round(teacher_words_spoken_per_min, 0)),
                y=.4,
                xref='x',
                yref='y',
                text='You speak {} words per minute'.format(round(teacher_words_spoken_per_min)),
                font=dict(
                    size=16),
                showarrow=True,
                arrowhead=7,
                ax=0,
                ay=-40
            ),
            dict(
                x=150,
                y=-.4,
                xref='x',
                yref='y',
                text='150.0 (wps) is the average rate of speaking',
                font=dict(
                    size=16),
                showarrow=True,
                arrowhead=7,
                ax=0,
                ay=40
            )
        ]
    )
    data_1 = [trace_]
    figure = dict(data=data_1, layout=layout_1)
    return figure, round(teacher_words_spoken_per_min)


def plot_jargon(jargonDF, sentenceDF):
    """
    Plot jargon words used
    :param jargonDF: df of jargon words used
    :return: dict ready for visualization
    """
    # Get jargon words and count
    top_ten = list(jargonDF.sort_values('tf', ascending=False).reset_index().loc[:9, "word"])
    jargon = jargonDF.shape[0]
    regular = get_word_count(sentenceDF)

    # Create annotations for each word
    annotations = [
        dict(
            x=.5,
            y=regular,
            xref='x',
            yref='y',
            text='Top Jargon Words Used',
            font=dict(
                size=14),
            showarrow=False
        )]
    increment = regular / 11
    for i, jword in enumerate(top_ten):
        jword_anno = dict(
            x=.5,
            y=regular - (increment * (i + 1)),
            xref='x',
            yref='y',
            text='{}. {}'.format(str(i + 1), jword),
            font=dict(
                size=14),
            showarrow=False
        )
        annotations.append(jword_anno)

    # Create plot
    trace0 = go.Bar(
        x=[0],
        y=[jargon],
        name='Jargon',
        width=.5,
        marker=go.bar.Marker(
            color='rgb(255,45,15)'
        )
    )

    trace1 = go.Bar(
        x=[0],
        y=[regular],
        name='Regular',
        width=.5,
        marker=dict(
            color='rgb(0, 59, 174)'
        )
    )

    data = [trace0, trace1]
    layout = go.Layout(
        title='Percent of Jargon Words',
        titlefont=dict(
            size=26),
        font=dict(
            family='serif'),
        barmode='stack',
        xaxis=dict(
            range=[0, .7],
            ticks='',
            showticklabels=False,
            showgrid=False
        ),
        yaxis=dict(
            ticks='',
            showticklabels=True,
            showgrid=False
        ),
        annotations=annotations
    )
    figure = dict(data=data, layout=layout)
    return figure


def plot_question(dialogueDF, sentenceDF):
    """
    Plot where questions occur
    :param dialogueDF: pd df returned from process_nlp
    :param sentenceDF: pd df returned from process_nlp
    :return: dict ready for visualization
    """
    # Prepare dataframe and get numbers
    num_q = pd.merge(dialogueDF, sentenceDF, on='sentence_index', how='outer', suffixes=('_dialogue', '_sentence'))
    num_q = num_q[['sentence_type', 'start_time', 'end_time']]
    whQuestion = num_q[num_q.sentence_type == 'whQuestion']
    ynQuestion = num_q[num_q.sentence_type == 'ynQuestion']
    num_q_only_qs = whQuestion.append(ynQuestion)
    num_q_pt = pd.pivot_table(num_q, index='sentence_type', values='start_time', aggfunc='count')
    total_qs = sum([int(x in ['ynQuestion', 'whQuestion']) for x in num_q['sentence_type'].tolist()])

    # Plot questions
    trace_questions = go.Scatter(
        x=list(num_q_only_qs['start_time'] / 60),
        y=[0.5 for i in list(num_q['start_time'])],
        mode='markers',
        name='Questions'
    )

    data = [trace_questions]
    layout = go.Layout(
        title='Questions Asked Over Time',
        titlefont=dict(
            size=26),
        font=dict(
            family='serif'),
        yaxis=dict(
            range=[0, 1],
            showgrid=False,
            zeroline=False,
            showline=False,
            ticks='',
            showticklabels=False
        ),
        xaxis=dict(
            title='Time (minutes)',
            titlefont=dict(
                size=18),
            showline=True
        ),
        annotations=[
            dict(
                x=float(list(num_q['start_time'] / 60)[-1]) / 2,
                y=0.75,
                xref='x',
                yref='y',
                text="There were {} questions asked during the lesson.".format(total_qs),
                font=dict(
                    size=16),
                showarrow=False
            ),
        ]
    )
    figure = dict(data=data, layout=layout)
    return figure, total_qs


def plot_proportions(sentenceDF, responsePause):
    """
    Plot proportions of teacher, students, pauses
    :param sentenceDF: pd df returned from process_nlp
    :param responsePause: pd df returned from process_nlp
    :return: dict ready for visualization, integer indicating which speaker is the teacher
    """
    sentenceDF['time_talked'] = sentenceDF['end_time'] - sentenceDF['start_time']
    pausesDF = responsePause[['start_time', 'end_time']]
    pausesDF['response_duration'] = pausesDF['end_time'] - pausesDF['start_time']

    sentence_pt = pd.pivot_table(sentenceDF, index=['speaker_label'], values=['time_talked'], aggfunc='sum')
    total_talk_and_pause = sentenceDF['time_talked'].sum() + pausesDF['response_duration'].sum()
    spk_0_percent = round((sentence_pt['time_talked'][0] / total_talk_and_pause) * 100, 2)
    try:
        spk_1_percent = round((sentence_pt['time_talked'][1] / total_talk_and_pause) * 100, 2)
    except Exception as e:
        spk_1_percent = 0.0
    paused_percent = round((pausesDF['response_duration'].sum() / total_talk_and_pause) * 100, 2)
    who_is_teacher = 0

    # Switch values so that the teacher is always longer
    if spk_1_percent > spk_0_percent:
        spk_0_percent, spk_1_percent = spk_1_percent, spk_0_percent
        who_is_teacher = 1

    trace_spk_0 = go.Bar(
        x=[0],
        y=[spk_0_percent],
        name='Teacher',
        text='Teacher',
        marker=dict(color='rgb(0, 59, 174)'),
    )

    trace_spk_1 = go.Bar(
        x=[0],
        y=[spk_1_percent],
        name='Students',
        text='Students',
        marker=dict(color='rgb(229, 83, 0)'),
    )

    trace_pauses = go.Bar(
        x=[0],
        y=[paused_percent],
        name='Paused',
        text='Paused'
    )

    layout_1 = go.Layout(
        barmode='stack',
        title='Percent of Time Talked by Teacher vs. Student vs. Time Spent Paused',
        titlefont=dict(
            size=26),
        font=dict(family='serif'),
        xaxis=dict(
            range=[0, 1],
            showline=False,
            showgrid=False,
            showticklabels=False
        ),
        yaxis=dict(
            range=[0, 100],
            showgrid=False,
            showline=True,
            showticklabels=True,
        ),
        annotations=[
            dict(
                x=.6,
                y=spk_0_percent / 2,
                xref='x',
                yref='y',
                text="The teacher spoke {0}% of the time.".format(spk_0_percent),
                font=dict(
                    size=16),
                showarrow=False,
            ),
            dict(
                x=.6,
                y=spk_0_percent + (spk_1_percent/2),
                xref='x',
                yref='y',
                text="Students spoke {0}% of the time.".format(spk_1_percent),
                font=dict(
                    size=16),
                showarrow=False,
            ),
            dict(
                x=.6,
                y=100 - paused_percent / 2,
                xref='x',
                yref='y',
                text="The lesson was paused {0}% of the time.".format(paused_percent),
                font=dict(
                    size=16),
                showarrow=False,
            )
        ]
    )

    data_1 = [trace_spk_0, trace_spk_1, trace_pauses]
    figure = dict(data=data_1, layout=layout_1)
    return figure, who_is_teacher


def plot_time(sentenceDF, responsePause, who_is_teacher):
    """
    Plot when speakers speak
    :param sentenceDF, responsePause: pd df returned from process_nlp
    :param who_is_teacher: integer indicating which speaker is the teacher
    :return: dict ready for visualization
    """
    spk_0_df = sentenceDF[sentenceDF.speaker_label == 'spk_0']
    spk_1_df = sentenceDF[sentenceDF.speaker_label == 'spk_1']
    pausesDF = responsePause[['start_time', 'end_time']]

    spk_0_start_end = list(spk_0_df['start_time']) + list(spk_0_df['end_time'])
    spk_1_start_end = list(spk_1_df['start_time']) + list(spk_1_df['end_time'])

    spk_0_x_data = []
    for i in range(len(spk_0_df['start_time'])):
        start = list(spk_0_df['start_time'])[i] / 60
        end = list(spk_0_df['end_time'])[i] / 60
        start_end = start, end
        spk_0_x_data.append(start_end)

    spk_1_x_data = []
    for i in range(len(spk_1_df['start_time'])):
        start = list(spk_1_df['start_time'])[i] / 60
        end = list(spk_1_df['end_time'])[i] / 60
        start_end = start, end
        spk_1_x_data.append(start_end)

    pauses_x_data = []
    for i in range(len(pausesDF['start_time'])):
        start = list(pausesDF['start_time'])[i] / 60
        end = list(pausesDF['end_time'])[i] / 60
        start_end = start, end
        pauses_x_data.append(start_end)

    # Switch values if teacher is speaker 1
    if who_is_teacher == 1:
        spk_0_x_data, spk_1_x_data = spk_1_x_data, spk_0_x_data

    # iterate over the start and end time data to create a plot that links the start and end time for each
    # instance in which someone speaks. For example, spk_1 from 7.141 - 7.521 are connected by a line and
    # the next time spk_1 speaks is from 19.341 - 21.361, and these points are connected by a line, but
    # there is a gap in between these two speaking instances.

    # create any empty list to append with graph data
    traces = []

    # spk_0 data
    # iterate over the list of start and end times to create separate traces for every speaking instance
    for i in range(len(spk_0_x_data)):
        # use only the first instance to create a legend, or there will be a legend entry for every speaking instance
        if i == 0:
            traces.append(go.Scatter(
                x=spk_0_x_data[i],
                y=[1 / 3, 1 / 3],
                mode='lines + markers',
                line=dict(color='rgb(0, 59, 174)'),
                marker=dict(color='rgb(0, 59, 174)'),
                name='Teacher'
            ))
        # hide the legend for the rest of the speaking instances so that there is only one legend entry for each speaker,
        # rather than each speaking instance
        else:
            traces.append(go.Scatter(
                x=spk_0_x_data[i],
                y=[1 / 3, 1 / 3],
                mode='lines + markers',
                line=dict(color='rgb(0, 59, 174)'),
                marker=dict(color='rgb(0, 59, 174)'),
                name='Teacher',
                showlegend=False
            ))

    # spk_1 data
    for i in range(len(spk_1_x_data)):
        # use only the first instance to create a legend, or there will be a legend entry for every speaking instance
        if i == 0:
            traces.append(go.Scatter(
                x=spk_1_x_data[i],
                y=[2 / 3, 2 / 3],
                mode='lines + markers',
                line=dict(color='rgb(229, 83, 0)'),
                marker=dict(color='rgb(229, 83, 0)'),
                name='Students'
            ))
        # hide the legend for the rest of the speaking instances so that there is only one legend entry for each speaker,
        # rather than each speaking instance
        else:
            traces.append(go.Scatter(
                x=spk_1_x_data[i],
                y=[2 / 3, 2 / 3],
                mode='lines + markers',
                line=dict(color='rgb(229, 83, 0)'),
                marker=dict(color='rgb(229, 83, 0)'),
                name='Students',
                showlegend=False
            ))

    # pause data
    for i in range(len(pauses_x_data)):
        # use only the first pause to create a legend, or there will be a legend entry for every pause
        if i == 0:
            traces.append(go.Scatter(
                x=pauses_x_data[i],
                y=[0.5, 0.5],
                mode='lines + markers',
                line=dict(color='rgb(282, 0, 0)'),
                marker=dict(color='rgb(282, 0, 0)', symbol='triangle-down'),
                name='Pause',
            ))
        # hide the legend for the rest of the pauses so that there is only one legend entry for all pauses,
        # rather than each pause
        else:
            traces.append(go.Scatter(
                x=pauses_x_data[i],
                y=[0.5, 0.5],
                mode='lines + markers',
                line=dict(color='rgb(282, 0, 0)'),
                marker=dict(color='rgb(282, 0, 0)', symbol='triangle-down'),
                name='Pause',
                showlegend=False
            ))

    data = traces
    layout = go.Layout(
        title='Teacher and Students Speaking Timeline',
        titlefont=dict(
            size=26),
        yaxis=dict(
            range=[0, 1],
            showgrid=False,
            zeroline=False,
            showline=False,
            ticks='',
            showticklabels=False
        ),
        xaxis=dict(
            title='Time (minutes)',
            titlefont=dict(
                size=18),
            showline=True
        ),
        font=dict(family='serif')
    )

    figure = go.Figure(data=data, layout=layout)
    return figure



def plot_emotion(emotionDF):
    # 1. Prepare data
    emotionDF['emotion_prob'] = emotionDF['emotion_prob'].replace(np.nan,0)
    emotionDF = emotionDF[emotionDF.emotion_prob > 0.5]
    emotionDF.reset_index(drop=True, inplace=True)
    emotionDF['minute'] = emotionDF['time'].apply(lambda x: int(x / 60) + 1)
    data_for_heatmap = emotionDF.groupby(['minute', 'emotion']).count()
    emotions = ['happy', 'neutral','surprise', 'sad', 'angry', 'fear']
    emotions_data = emotions_data_for_heatmap(emotions, range(len(list(np.unique(emotionDF['minute'])))),
                                              data_for_heatmap)
    max_val = max(i for v in emotions_data.values() for i in v)

    # Create plot
    trace = go.Heatmap(
        z=[
            emotions_data['happy'],
            emotions_data['neutral'],
            emotions_data['surprise'],
            emotions_data['sad'],
            emotions_data['angry'],
            emotions_data['fear']
        ],
        x=[i for i in list(np.unique(emotionDF['minute']))],
        y=emotions,
        colorbar=dict(
            tickmode='array',
            tickvals=[5, (max_val -5)],
            ticks='',
            ticktext=['Infreqent', 'Freqent']
        )
    )

    data = [trace]

    layout = go.Layout(
        title='Frequency of Emotions Displayed During the Class by Minute',
        titlefont=dict(
            size=24),
        xaxis=dict(
            title='Time (minutes)',
            titlefont=dict(
                size=18),
        ),
        yaxis=dict(
            title='Emotions',
            titlefont=dict(
                size=18),
        ),
        font=dict(family='serif')
    )

    figure = dict(data=data, layout=layout)
    return figure


def emotions_data_for_heatmap(emotions_list, length_of_video, data):
    emotions_data = defaultdict(lambda: [])
    for emo in emotions_list:
        for i in range(len(length_of_video)):
            try:
                emotions_data[emo].append(data.loc[i+1]['username']['{}'.format(emo)])
            except Exception as e:
                emotions_data[emo].append(0)
    return emotions_data

def plot_occurrence(insult_lang_df, laugh_df, yell_df, applause_df):
    # respectful Language Data
    if len(insult_lang_df) > 0:
        trace_insult_lang = go.Scatter(
            x=list(insult_lang_df['start_time'] / 60),
            y=[0.25 for i in list(insult_lang_df['start_time'])],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Insulting Language',
            showlegend=True

        )
    else:
        trace_insult_lang = go.Scatter(
            x=[0],
            y=[0],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Insulting Language',
            showlegend=True,
            visible= "legendonly"
        )
    # yelling Data
    if len(yell_df) > 0:
        trace_yell = go.Scatter(
            x=list(yell_df['start_time'] / 60),
            y=[0.75 for i in list(yell_df['start_time'])],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Yelling',
            showlegend=True
        )
    else:
        trace_yell = go.Scatter(
            x=[0],
            y=[0],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Yelling',
            showlegend=True,
            visible="legendonly"
        )



    # laughing Data
    if len(laugh_df) > 0:
        trace_laugh = go.Scatter(
            x=list(laugh_df['start_time'] / 60),
            y=[0.5 for i in list(laugh_df['start_time'])],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Laughing',
            showlegend=True
        )
    else:
        trace_laugh = go.Scatter(
            x=[0],
            y=[0],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Laughing',
            showlegend=True,
            visible="legendonly"
        )

    # applause data
    if len(applause_df) > 0:
        trace_applause = go.Scatter(
            x=list(applause_df['start_time'] / 60),
            y=[0.25 for i in list(applause_df['start_time'])],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Applause',
            showlegend=True
        )
    else:
        trace_applause = go.Scatter(
            x=[0],
            y=[0],
            mode='markers',
            #     marker = dict(color='rgb(0, 59, 174)'
            #     ),
            name='Applause',
            showlegend=True,
            visible="legendonly"
        )



    data = [trace_insult_lang, trace_yell, trace_laugh, trace_applause]
    layout = go.Layout(
        title='Timeline Of Occurrences',
        titlefont=dict(
            size=26),
        xaxis=dict(
            zeroline=True,
            showline=True,
            title='Time (minutes)'
        ),
        yaxis=dict(
            range=[0, 1],
            showgrid=False,
            zeroline=False,
            showline=False,
            ticks='',
            showticklabels=False
        ),
        font=dict(family='serif')
    )

    fig = dict(data=data, layout=layout)
    return fig

def get_data_for_occurrence(sentence, insults, laughter, yelling, applause):
    insult_lang_df = pd.merge(insults, sentence, how='outer', on='sentence_index',
                                    suffixes=('_insults', '_sentence'))
    insult_lang_df = insult_lang_df[insult_lang_df.insult_rating > 0.7]
    insult_lang_df = insult_lang_df[['insult_rating', 'start_time', 'end_time']]
    laugh_df = laughter[['start_time']]
    applause_df = applause[['start_time']]
    yell_df = yelling[['start_time', 'yelling']]
    yell_df = yell_df[yell_df.yelling == True]

    return insult_lang_df, laugh_df, yell_df, applause_df


def pause_analysis(sentenceDF, responsePause):
    """
    Return average pause time disaggregated by teacher's why or yes-no question
    and teacher or student response
    :param sentenceDF: pd df returned from process_nlp
    :param responsePause: pd df returned from process_nlp
    :return: dictionary of average values
    """
    pause_data = {'why_student': None, 'yn_student': None, 'why_teacher': None, 'yn_teacher': None}
    try:
        teacher = get_teacher(sentenceDF)[1]
        teacherDF = responsePause[responsePause['speaker_label'] == teacher]
        question_table = pd.pivot_table(teacherDF, values='response_duration', index=['type'], columns=['same_speaker'], aggfunc=np.mean)
        pause_data['why_student'] = round(question_table.loc['whQuestion', 0], 2)
        pause_data['yn_student'] = round(question_table.loc['ynQuestion', 0], 2)
        pause_data['why_teacher'] = round(question_table.loc['whQuestion', 1], 2)
        pause_data['yn_teacher'] = round(question_table.loc['ynQuestion', 0], 2)
    finally:
        return pause_data
