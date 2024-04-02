import boto3
import json
import sys
import time


results = []


def words_convert(word):
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
                    # JobId=[self.startJobId_detection,self.startJobId_search]
                    JobId_detection = self.startJobId_detection
                    JobId_search = self.startJobId_search
                    jobId_to_use = JobId_detection if rekMessage['JobId'] == JobId_detection else JobId_search
                    if rekMessage['JobId'] in [JobId_detection, JobId_search]:
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

        while finished == False:
            response = self.rek.get_face_detection(JobId=self.startJobId_detection,
                                                   MaxResults=maxResults,
                                                   NextToken=paginationToken
                                                   )

            print('Codec: ' + response['VideoMetadata']['Codec'])
            print('Duration: ' +
                  str(response['VideoMetadata']['DurationMillis']))
            print('Format: ' + response['VideoMetadata']['Format'])
            print('Frame rate: ' + str(response['VideoMetadata']['FrameRate']))
            print()
            for faceDetection in response['Faces']:

                # print('Face: ' + str(faceDetection['Face']))
                faceDetails = faceDetection['Face']
                print(f"Confidence: {faceDetails['Confidence']:.2f}%")

                print(f"Timestamp:  {str(faceDetection['Timestamp'])} (ms)")
                print(
                    f"性別: {words_convert(str(faceDetails['Gender']['Value']))} ({faceDetails['Gender']['Confidence']:.2f}%)")
                print(
                    f"年齡區間: {str(faceDetails['AgeRange']['Low'])}~{str(faceDetails['AgeRange']['High'])}")
                emotions = [
                    f"{words_convert(emotion['Type'])}({emotion['Confidence']:.2f}%)" for emotion in faceDetails['Emotions']]
                print(f"情绪: {', '.join(emotions)}")
                print()

            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True
        # return results

    def GetFaceSearchCollectionResults(self):
        maxResults = 10
        paginationToken = ''
        video_info = False
        finished = False
        while finished == False:
            response = self.rek.get_face_search(JobId=self.startJobId_search,
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
                        print("   Face ID: " + face['FaceId'])
                        print("   相似度: " + str(faceMatch['Similarity']))
                        print(
                            f"   姓名: {chinese_name(face['ExternalImageId'])}")
                        print()
                else:
                    print("   警告!未知人臉")
                    print()
                if 'NextToken' in response:
                    paginationToken = response['NextToken']
                else:
                    finished = True

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
        analyzer.GetFaceDetectionResults()
        analyzer.GetFaceSearchCollectionResults()

        # bounding_boxes = analyzer.GetFaceDetectionResults()
        # DrawBoundingBox(bucket, video, bounding_boxes)
    analyzer.DeleteTopicandQueue()


if __name__ == "__main__":
    main()
