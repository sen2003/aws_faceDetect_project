# 把detection和search整合在一起
import boto3
import json
import sys
import time
import cv2


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
                            "Timestamp_search": personMatch['Timestamp'],
                            "BoundingBox": personFace['BoundingBox'],
                            'Name': name_convert(face['ExternalImageId']),
                            'Similarity': faceMatch['Similarity']
                        }
                        search_results.append(search_data_match)
                else:
                    search_data_no_match = {
                        "Timestamp_search": personMatch['Timestamp'],
                        "BoundingBox": personFace['BoundingBox'],
                        'Name': 'Unkuow',
                        'Similarity': 0.00
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

    def GetFinalResult(self, detection_results, search_results):
        search_results_dict = {
            int(result['Timestamp_search']): result for result in search_results}

        for detection in detection_results:
            timestamp_detection = int(detection['Timestamp_detection'])

            match_range = 100
            matched_search_result = None

            for timestamp_search in range(timestamp_detection - match_range, timestamp_detection + match_range + 1):
                if timestamp_search in search_results_dict:
                    matched_search_result = search_results_dict[timestamp_search]
                    break

            if matched_search_result:
                print(
                    f"Detection Timestamp: {detection['Timestamp_detection']}")
                print(
                    f"Search Timestamp: {matched_search_result['Timestamp_search']}")
                print(
                    f"姓名: {matched_search_result['Name']} ({matched_search_result['Similarity']:.2f}%)")
                print(
                    f"性別: {detection['Gender']['Value']} ({detection['Gender']['Confidence']:.2f}%)")
                print(
                    f"年齡區間: {detection['AgeRange']['Low']}-{detection['AgeRange']['Heigh']}")
                print(f"情緒: {' , '.join(detection['Emotions'])}")

                print(
                    "-------------------------------------------------------------------------------------------------------------")
            else:
                print(
                    f"Detection Timestamp: {detection['Timestamp_detection']} - No matching face found in search results.")
                print(
                    "-------------------------------------------------------------------------------------------------------------")


def DrawBoundingBox(bucket, video, detection_results, search_results):
    tmp_filename = './input.mp4'

    s3_client = boto3.resource('s3')
    s3_client.meta.client.download_file(bucket, video, tmp_filename)

    cap = cv2.VideoCapture(tmp_filename)
    fps = cap.get(cv2.CAP_PROP_FPS)
    img_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    img_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video = cv2.VideoWriter('./output.mp4', fourcc,
                            fps, (img_width, img_height))
    cur_idx = 0
    left = int(search_results[cur_idx]['BoundingBox']['Left'] * img_width)
    top = int(search_results[cur_idx]['BoundingBox']['Top'] * img_height)
    right = int((search_results[cur_idx]['BoundingBox']['Left'] +
                search_results[cur_idx]['BoundingBox']['Width']) * img_width)
    bottom = int((search_results[cur_idx]['BoundingBox']['Top'] +
                 search_results[cur_idx]['BoundingBox']['Height']) * img_height)

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        timestamp = cap.get(cv2.CAP_PROP_POS_MSEC)
        if cur_idx < len(search_results) - 1 and timestamp >= search_results[cur_idx + 1]['Timestamp_search']:
            cur_idx += 1
            left = int(search_results[cur_idx]
                       ['BoundingBox']['Left'] * img_width)
            top = int(search_results[cur_idx]
                      ['BoundingBox']['Top'] * img_height)
            right = int((search_results[cur_idx]['BoundingBox']['Left'] +
                        search_results[cur_idx]['BoundingBox']['Width']) * img_width)
            bottom = int((search_results[cur_idx]['BoundingBox']['Top'] +
                         search_results[cur_idx]['BoundingBox']['Height']) * img_height)

        img = cv2.rectangle(frame, (left, top), (right, bottom), (255, 0, 0))
        cv2.putText(img, search_results[cur_idx]['Name'], (left, top-10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.9, (36, 255, 12), 2)
        video.write(frame)

    cap.release()
    video.release()


def main():

    roleArn = 'arn:aws:iam::637423267378:role/LabRole'
    bucket = 'lab-video-search'
    video = 'video_detect03.mp4'

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
        analyzer.GetFinalResult(detection_results, search_results)
        DrawBoundingBox(bucket, video, detection_results, search_results)
    analyzer.DeleteTopicandQueue()


if __name__ == "__main__":
    main()
