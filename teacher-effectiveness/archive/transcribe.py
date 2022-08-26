import argparse
import boto3
import csv
import json
import os.path
import time
from datetime import datetime
from urllib import request



def upload_to_s3(file_name, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
    sts = {"status": 0}
    try:
        s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                          region_name=region_name)
        buckets = s3.list_buckets()
        if len([bucket for bucket in buckets["Buckets"] if bucket["Name"] == bucket_name]) == 0:
            s3.create_bucket(Bucket=bucket_name)
            print("Bucket '{}' was created".format(bucket_name))
        else:
            print("Bucket '{}' already exists".format(bucket_name))
        bucket_content = s3.list_objects_v2(Bucket=bucket_name)
        file_key = "audio/{}".format(os.path.basename(file_name))
        if bucket_content["KeyCount"] == 0 or \
                len([f for f in bucket_content["Contents"] if f["Key"] == file_key]) == 0:
            s3.upload_file(file_name, bucket_name, file_key)
            print("File '{}' was uploaded".format(file_name))
        else:
            print("File '{}' already exists".format(file_name))
            sts["file_uri"] = "{}/{}/{}".format(s3.meta.endpoint_url, bucket_name, file_key)
    except Exception as e:
        sts["status"] = 1
        print(str(e))
    return sts


def transcribe_audio(file_uri, speakers, aws_access_key_id, aws_secret_access_key, region_name):
    sts = {"status": 0}
    try:
        audio_format = file_url.split(".")[-1].lower()
        transcribe = boto3.client('transcribe', aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key, region_name=region_name)
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
        sts["transcription"] = json.loads(request.urlopen(
            transcription_job["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]).read())
    except Exception as e:
        sts["status"] = 1
        print(str(e))
    return sts


def transcribe_to_csv(transcription, file_name):
    terms = [x for x in transcription["results"]["items"] if x["type"] == "pronunciation"]
    if "speaker_labels" in transcription["results"].keys():
        speakers = [y for x in transcription["results"]["speaker_labels"]["segments"] for y in x["items"]]
    else:
        speakers = [{"speaker_label": "na"}]*len(terms)
    with open(file_name, 'w', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "speaker", "term", "confidence", "type", "start_time", "end_time"])
        for i, (term, speaker) in enumerate(zip(terms, speakers)):
            row = [i,
                   speaker["speaker_label"],
                   term["alternatives"][0]["content"],
                   term["alternatives"][0].get("confidence", 1),
                   term["type"],
                   term["start_time"],
                   term["end_time"]]
            writer.writerow(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", type=str, default="audio/MLKDream_64kb.mp3",
                        help="Audio file to process.")
    parser.add_argument("-o", "--output_csv", type=str, default=None,
                        help="The name of the output file.")
    parser.add_argument("-s", "--speakers", type=int, default=2,
                        help="The maximum number of speakers to identify in the input audio.")
    parser.add_argument("--aws_access_key_id", type=str, default=None,
                        help="The access key to use when creating the client.")
    parser.add_argument("--aws_secret_access_key", type=str, default=None,
                        help="The secret key to use when creating the client.")
    parser.add_argument("--region_name", type=str, default="us-east-1",
                        help="The name of the region associated with the client.")
    args = parser.parse_args()

    audio_format = "mp3"
    bucket_name = "classroom-bucket"

    # s3
    s3_status = upload_to_s3(args.file, bucket_name, args.aws_access_key_id,
                             args.aws_secret_access_key, args.region_name)

    # transcribe
    transcribe_status = transcribe_audio(s3_status["file_uri"], args.speakers, args.aws_access_key_id,
                                         args.aws_secret_access_key, args.region_name)

    # export to csv
    transcribe_to_csv(transcribe_status["transcription"],
                      args.output_csv or "{}.csv".format(os.path.basename(args.file)))

    # comprehend

    # comprehend = boto3.client('comprehend')
    #
    # n = 5000
    # documents = [transcribe_status["transcription"][i:i + n] for i in range(0, len(transcribe_status["transcription"]), n)]
    # print(documents)
    #
    # key_phrases_response = comprehend.batch_detect_key_phrases(TextList=documents, LanguageCode='en')
    # key_phrases = [k for l in key_phrases_response["ResultList"] for k in l["KeyPhrases"]]
    # key_text = "; ".join([k["Text"] for k in key_phrases])
    #
    # print(key_text)
    #
    # sentiment_response = comprehend.batch_detect_sentiment(TextList=documents, LanguageCode='en')
    #
    # print("Done")