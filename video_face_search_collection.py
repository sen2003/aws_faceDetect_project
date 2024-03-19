# 將影片與集合中的人臉做比對
import boto3
import json
import sys
import time
import cv2


def chinese_name(externalImageId):
    name_map = {
        "Huang": "黃士熏",
        "Ke": "柯信汶",
        "Shen": "沈宏勳",
        "Tsou": "鄒博森"
    }
    return name_map.get(externalImageId, "Unknow person")


class VideoDetect:

    jobId = ''
    roleArn = ''
    bucket = ''
    video = ''
    startJobId = ''
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
                    if rekMessage['JobId'] == self.startJobId:
                        print('Matching Job Found:' + rekMessage['JobId'])
                        jobFound = True
                        if (rekMessage['Status'] == 'SUCCEEDED'):
                            succeeded = True

                        self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                                ReceiptHandle=message['ReceiptHandle'])
                    else:
                        print("Job didn't match:" +
                              str(rekMessage['JobId']) + ' : ' + self.startJobId)
                    # Delete the unknown message. Consider sending to dead letter queue
                    self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                            ReceiptHandle=message['ReceiptHandle'])

        return succeeded

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

        self.startJobId = response['JobId']
        print('Start Job Id: ' + self.startJobId)

    def GetFaceSearchCollectionResults(self):
        maxResults = 10
        paginationToken = ''
        video_info = False
        finished = False
        results = []
        while finished == False:
            response = self.rek.get_face_search(JobId=self.startJobId,
                                                MaxResults=maxResults,
                                                NextToken=paginationToken,
                                                )
            if not video_info:
                print('Codec: ' + response['VideoMetadata']['Codec'])
                print('Duration: ' +
                      str(response['VideoMetadata']['DurationMillis']))
                print('Format: ' + response['VideoMetadata']['Format'])
                print('Frame rate: ' +
                      str(response['VideoMetadata']['FrameRate']))
                print()
                video_info = True

            for personMatch in response['Persons']:
                print("Timestamp: " + str(personMatch['Timestamp']))
                if 'FaceMatches' in personMatch and len(personMatch['FaceMatches']) > 0:
                    for faceMatch in personMatch['FaceMatches']:
                        face = faceMatch['Face']
                        face['BoundingBox']['top'] = face['BoundingBox']['Top']
                        face['BoundingBox']['left'] = face['BoundingBox']['Left']
                        face['BoundingBox']['width'] = face['BoundingBox']['Width']
                        face['BoundingBox']['height'] = face['BoundingBox']['Height']
                        results.append(
                            {'Timestamp': personMatch['Timestamp'], 'BoundingBox': face['BoundingBox']})
                        print("   Face ID: " + face['FaceId'])
                        print("   相似度: " + str(faceMatch['Similarity']))
                        print(
                            f"   姓名: {chinese_name(face['ExternalImageId'])}")
                        print("   Bounding Box:")
                        print("       Top: " + str(face['BoundingBox']['Top']))
                        print("       Left: " +
                              str(face['BoundingBox']['Left']))
                        print("       Width: " +
                              str(face['BoundingBox']['Width']))
                        print("       Height: " +
                              str(face['BoundingBox']['Height']))
                        print()
                else:
                    print("   警告!未知人臉")
                    print()

                if 'NextToken' in response:
                    paginationToken = response['NextToken']
                else:
                    finished = True
        return results

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


def DrawBoundingBox(bucket, video_s3, bounding_boxes):
    temp_file = './temp.mp4'
    s3_client = boto3.resource('s3')
    s3_client.meta.client.download_file(bucket, video_s3, temp_file)

    cap = cv2.VideoCapture(temp_file)
    fps = cap.get(cv2.CAP_PROP_FPS)
    img_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    img_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fourcc = cv2.VideoWriter_fourcc('m', 'p', '4', 'v')
    video = cv2.VideoWriter('./output.mp4', fourcc,
                            fps, (img_width, img_height))

    cur_idx = 0
    left = int(bounding_boxes[cur_idx]['BoundingBox']
               ['Left'] * img_width)  # 左上x
    top = int(bounding_boxes[cur_idx]['BoundingBox']
              ['Top'] * img_height)  # 左上y
    right = int((bounding_boxes[cur_idx]['BoundingBox']['Left'] +
                bounding_boxes[cur_idx]['BoundingBox']['Width']) * img_width)  # 右下x
    bottom = int((bounding_boxes[cur_idx]['BoundingBox']['Top'] +
                 bounding_boxes[cur_idx]['BoundingBox']['Height']) * img_height)  # 右下y

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        timestamp = cap.get(cv2.CAP_PROP_POS_MSEC)
        for bbox in bounding_boxes:
            if timestamp >= bbox['Timestamp']:
                # 计算边框的位置
                left = int(bbox['BoundingBox']['Left'] * img_width)
                top = int(bbox['BoundingBox']['Top'] * img_height)
                width = int(bbox['BoundingBox']['Width'] * img_width)
                height = int(bbox['BoundingBox']['Height'] * img_height)
                right = left + width
                bottom = top + height

                # 在帧上绘制边框
                cv2.rectangle(frame, (left, top),
                              (right, bottom), (0, 255, 0), 2)

        video.write(frame)

    cap.release()
    video.release()


def main():

    roleArn = 'arn:aws:iam::637423267378:role/LabRole'
    bucket = 'lab-video-search'
    video_s3 = 'video_detect04.mp4'

    session = boto3.Session(profile_name='default')
    client = session.client('rekognition')
    rek = boto3.client('rekognition')
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')

    analyzer = VideoDetect(roleArn, bucket, video_s3, client, rek, sqs, sns)
    analyzer.CreateTopicandQueue()

    collection = 'myCollection1'
    analyzer.StartFaceSearchCollection(collection)
    if analyzer.GetSQSMessageSuccess() == True:
        bounding_boxes = analyzer.GetFaceSearchCollectionResults()
        DrawBoundingBox(bucket, video_s3, bounding_boxes)

    analyzer.DeleteTopicandQueue()


if __name__ == "__main__":
    main()
