# 把detection和search整合在一起
import boto3
import json
import sys
import time
import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont


def word_convert(word):
    word_map = {
        "Male": "男性",
        "Female": "女性",
        "CALM": "平靜",
        "HAPPY": "開心",
        "SAD": "傷心",
        "CONFUSED": "困惑",
        "SURPRISED": "驚訝",
        "ANGRY": "憤怒",
        "DISGUSTED": "厭惡",
        "FEAR": "恐懼"
    }
    return word_map.get(word, "unknow")


def name_convert(externalImageId):
    name_map = {
        "Huang": "黃士熏",
        "Ke": "柯信汶",
        "Shen": "沈宏勳",
        "Tsou": "鄒博森"
    }
    return name_map.get(externalImageId, "Unknow Person")


class VideoDetect:

    jobId = ''

    roleArn = ''
    bucket = ''
    video = ''
    startJobId_search = ''
    startJobId_detection = ''

    sqsQueueUrl = ''
    snsTopicArn = ''
    processType = ''

    def __init__(self, role, bucket, video, client, rek, sqs, sns):
        self.roleArn = role
        self.bucket = bucket
        self.video = video
        self.client = client
        self.rek = rek
        self.sqs = sqs
        self.sns = sns

    def GetSQSMessageSuccess(self):

        jobFound = False
        succeeded = False

        dotLine = 0
        while jobFound == False:
            sqsResponse = self.sqs.receive_message(QueueUrl=self.sqsQueueUrl, MessageAttributeNames=['ALL'],
                                                   MaxNumberOfMessages=10)

            if sqsResponse:

                if 'Messages' not in sqsResponse:
                    if dotLine < 40:
                        print('.', end='')
                        dotLine = dotLine + 1
                    else:
                        print()
                        dotLine = 0
                    sys.stdout.flush()
                    time.sleep(5)
                    continue

                for message in sqsResponse['Messages']:
                    notification = json.loads(message['Body'])
                    rekMessage = json.loads(notification['Message'])
                    print(rekMessage['JobId'])
                    print(rekMessage['Status'])
                    JobId_detection = self.startJobId_detection
                    JobId_search = self.startJobId_search
                    jobId_to_use = JobId_detection if rekMessage['JobId'] == JobId_detection else JobId_search
                    if rekMessage['JobId'] == JobId_detection or rekMessage['JobId'] == JobId_search:
                        print('Matching Job Found:' + rekMessage['JobId'])
                        jobFound = True
                        if (rekMessage['Status'] == 'SUCCEEDED'):
                            succeeded = True

                        self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                                ReceiptHandle=message['ReceiptHandle'])
                    else:
                        print("Job didn't match:" +
                              str(rekMessage['JobId']) + ' : ' + jobId_to_use)
                    # Delete the unknown message. Consider sending to dead letter queue
                    self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                            ReceiptHandle=message['ReceiptHandle'])
        return succeeded

    def StartFaceDetection(self):
        response = self.rek.start_face_detection(Video={'S3Object': {'Bucket': self.bucket, 'Name': self.video}},
                                                 NotificationChannel={'RoleArn': self.roleArn, 'SNSTopicArn': self.snsTopicArn}, FaceAttributes='ALL')

        self.startJobId_detection = response['JobId']
        print('Start Job Id(detection): ' + self.startJobId_detection)

    def StartFaceSearchCollection(self, collection):
        response = self.rek.start_face_search(Video={'S3Object': {'Bucket': self.bucket, 'Name': self.video}},
                                              CollectionId=collection,
                                              NotificationChannel={'RoleArn': self.roleArn,
                                                                   'SNSTopicArn': self.snsTopicArn},
                                              FaceMatchThreshold=0,
                                              # Filtration options, uncomment and add desired labels to filter returned labels
                                              # Features=['GENERAL_LABELS'],
                                              # Settings={
                                              # 'GeneralLabels': {
                                              # 'LabelInclusionFilters': ['Clothing']
                                              # }}
                                              )

        self.startJobId_search = response['JobId']
        print('Start Job Id(search): ' + self.startJobId_search)

    def GetFaceDetectionResults(self):
        maxResults = 10
        paginationToken = ''
        finished = False
        detection_results = []

        while finished == False:
            response = self.rek.get_face_detection(JobId=self.startJobId_detection,
                                                   MaxResults=maxResults,
                                                   NextToken=paginationToken
                                                   )

            # print('Codec: ' + response['VideoMetadata']['Codec'])
            # print('Duration: ' +
            #       str(response['VideoMetadata']['DurationMillis']))
            # print('Format: ' + response['VideoMetadata']['Format'])
            # print('Frame rate: ' + str(response['VideoMetadata']['FrameRate']))
            # print()
            for faceDetection in response['Faces']:
                # print('Face: ' + str(faceDetection['Face']))
                faceDetails = faceDetection['Face']
                emotions = [
                    f"{word_convert(emotion['Type'])}({emotion['Confidence']:.2f}%)" for emotion in faceDetails['Emotions']]
                detection_data = {
                    'Timestamp_detection': faceDetection['Timestamp'],
                    'Gender': {'Value': word_convert(faceDetails['Gender']['Value']),
                               'Confidence': faceDetails['Gender']['Confidence']},
                    'AgeRange': {
                        'Low': str(faceDetails['AgeRange']['Low']), 'Heigh': str(faceDetails['AgeRange']['High'])},
                    'Emotions': emotions,
                    'BoundingBox': faceDetection['Face']['BoundingBox']
                }
                detection_results.append(detection_data)

            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True
        return detection_results

    def GetFaceSearchCollectionResults(self):
        maxResults = 10
        paginationToken = ''
        # video_info = False
        finished = False
        search_results = []
        while finished == False:
            response = self.rek.get_face_search(JobId=self.startJobId_search,
                                                MaxResults=maxResults,
                                                NextToken=paginationToken,
                                                )
            # if not video_info:
            #     print('Codec: ' + response['VideoMetadata']['Codec'])
            #     print('Duration: ' +
            #           str(response['VideoMetadata']['DurationMillis']))
            #     print('Format: ' + response['VideoMetadata']['Format'])
            #     print('Frame rate: ' +
            #           str(response['VideoMetadata']['FrameRate']))
            #     print()
            #     video_info = True

            for personMatch in response['Persons']:
                personFace = personMatch["Person"]["Face"]
                # bounding_box = face['BoundingBox']
                # print("Timestamp: " + str(personMatch['Timestamp']))

                if 'FaceMatches' in personMatch and len(personMatch['FaceMatches']) > 0:
                    for faceMatch in personMatch['FaceMatches']:
                        face = faceMatch['Face']
                        search_data_match = {
                            'Timestamp_search': personMatch['Timestamp'],
                            "BoundingBox": personFace['BoundingBox'],
                            'Name': name_convert(face['ExternalImageId']),
                            'Similarity': faceMatch['Similarity']
                        }
                        search_results.append(search_data_match)
                else:
                    search_data_no_match = {
                        'Timestamp_search': personMatch['Timestamp'],
                        'BoundingBox': personFace['BoundingBox'],
                        'Name': "Unknow",
                        'Similarity': 0
                    }

                    search_results.append(search_data_no_match)

                    # print("   未知人臉")
                    # print()
                if 'NextToken' in response:
                    paginationToken = response['NextToken']
                else:
                    finished = True
        return search_results

    def CreateTopicandQueue(self):

        millis = str(int(round(time.time() * 1000)))

        # Create SNS topic

        snsTopicName = "AmazonRekognitionExample" + millis

        topicResponse = self.sns.create_topic(Name=snsTopicName)
        self.snsTopicArn = topicResponse['TopicArn']

        # create SQS queue
        sqsQueueName = "AmazonRekognitionQueue" + millis
        self.sqs.create_queue(QueueName=sqsQueueName)
        self.sqsQueueUrl = self.sqs.get_queue_url(
            QueueName=sqsQueueName)['QueueUrl']

        attribs = self.sqs.get_queue_attributes(QueueUrl=self.sqsQueueUrl,
                                                AttributeNames=['QueueArn'])['Attributes']

        sqsQueueArn = attribs['QueueArn']

        # Subscribe SQS queue to SNS topic
        self.sns.subscribe(
            TopicArn=self.snsTopicArn,
            Protocol='sqs',
            Endpoint=sqsQueueArn)

        # Authorize SNS to write SQS queue
        policy = """{{
  "Version":"2012-10-17",
  "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
  ]
}}""".format(sqsQueueArn, self.snsTopicArn)

        response = self.sqs.set_queue_attributes(
            QueueUrl=self.sqsQueueUrl,
            Attributes={
                'Policy': policy
            })

    def DeleteTopicandQueue(self):
        self.sqs.delete_queue(QueueUrl=self.sqsQueueUrl)
        self.sns.delete_topic(TopicArn=self.snsTopicArn)

    # def GetFinalResult(self, detection_results, search_results):
    #     search_results_dict = {
    #         int(result['Timestamp_search']): result for result in search_results}

    #     for detection in detection_results:
    #         timestamp_detection = int(detection['Timestamp_detection'])

    #         match_range = 100
    #         matched_search_result = None

    #         for timestamp_search in range(timestamp_detection - match_range, timestamp_detection + match_range + 1):
    #             if timestamp_search in search_results_dict:
    #                 matched_search_result = search_results_dict[timestamp_search]
    #                 break

        # if matched_search_result:
        #     print(
        #         f"Detection Timestamp: {detection['Timestamp_detection']}")
        #     print(
        #         f"Search Timestamp: {matched_search_result['Timestamp_search']}")
        #     print(
        #         f"姓名: {matched_search_result['Name']} ({matched_search_result['Similarity']:.2f}%)")
        #     print(
        #         f"性別: {detection['Gender']['Value']} ({detection['Gender']['Confidence']:.2f}%)")
        #     print(
        #         f"年齡區間: {detection['AgeRange']['Low']}-{detection['AgeRange']['Heigh']}")
        #     print(f"情緒: {' , '.join(detection['Emotions'])}")

        #     print(
        #         "------------------------------------------------------------------------------------------------------------------")
        # else:
        #     print(
        #         f"Detection Timestamp: {detection['Timestamp_detection']} - No matching face found in search results.")
        #     print(
        #         "------------------------------------------------------------------------------------------------------------------")


def cv2ChineseText(img, text, position, textColor, textSize):
    if (isinstance(img, np.ndarray)):
        img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
    draw = ImageDraw.Draw(img)
    fontStyle = ImageFont.truetype("/msjhbd.ttc", textSize, encoding="utf-8")
    draw.text(position, text, textColor, font=fontStyle)
    return cv2.cvtColor(np.asanyarray(img), cv2.COLOR_RGB2BGR)


def DrawBoundingBox(bucket, video, search_results):
    tmp_filename = './input.mp4'

    s3_client = boto3.resource('s3')
    s3_client.meta.client.download_file(bucket, video, tmp_filename)

    cap = cv2.VideoCapture(tmp_filename)
    fps = cap.get(cv2.CAP_PROP_FPS)
    img_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    img_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    output_video = cv2.VideoWriter(
        './output.mp4', fourcc, fps, (img_width, img_height))

    active_faces = []  # 活动的人脸列表

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        timestamp = cap.get(cv2.CAP_PROP_POS_MSEC)

        # 更新或保持人脸框信息
        for search_result in search_results:
            if abs(timestamp - search_result['Timestamp_search']) < (1000 / fps):
                update = False
                for face in active_faces:
                    if face['Name'] == search_result['Name']:
                        face['BoundingBox'] = search_result['BoundingBox']
                        face['Timestamp'] = timestamp
                        face['Timestamp_search'] = search_result['Timestamp_search']
                        face['Similarity'] = search_result['Similarity']
                        # face['Name'] = search_result['Name']
                        update = True
                        break
                if not update:
                    # 添加新的人脸框
                    active_faces.append(search_result)

        active_faces = [
            face for face in active_faces if face['Name'] != 'Unknow']
        # active_faces = [
        #     face for face in active_faces if face['Timestamp'] <= timestamp]

        for face in active_faces:
            left = int(face['BoundingBox']['Left'] * img_width)
            top = int(face['BoundingBox']['Top'] * img_height)
            right = int((face['BoundingBox']['Left'] +
                        face['BoundingBox']['Width']) * img_width)
            bottom = int((face['BoundingBox']['Top'] +
                          face['BoundingBox']['Height']) * img_height)
            cv2.rectangle(frame, (left, top), (right, bottom), (0, 0, 255), 2)
            frame = cv2ChineseText(
                frame, f"{face['Name']}({face['Similarity']:.2f}%)", (left, top - 30), (36, 255, 12), 24)
            if (face['Timestamp_search'] == search_results[-1]['Timestamp_search']):
                active_faces = []
        output_video.write(frame)
    cap.release()
    output_video.release()


def main():

    roleArn = 'arn:aws:iam::637423267378:role/LabRole'
    bucket = 'lab-video-search'
    video = 'video_detect04.mp4'

    session = boto3.Session(profile_name='default')
    client = session.client('rekognition')
    rek = boto3.client('rekognition')
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')

    analyzer = VideoDetect(roleArn, bucket, video, client, rek, sqs, sns)
    analyzer.CreateTopicandQueue()

    collection = 'myCollection1'
    analyzer.StartFaceDetection()
    analyzer.StartFaceSearchCollection(collection)
    if analyzer.GetSQSMessageSuccess() == True:
        detection_results = analyzer.GetFaceDetectionResults()
        search_results = analyzer.GetFaceSearchCollectionResults()
        # analyzer.GetFinalResult(detection_results, search_results)
        # results_json = json.dumps(final_result, indent=4, ensure_ascii=False)
        # with open('detection_results.json', 'w', encoding='utf-8') as f:
        #     f.write(results_json)
        DrawBoundingBox(bucket, video, search_results)

    analyzer.DeleteTopicandQueue()


if __name__ == "__main__":
    main()
