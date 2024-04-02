# 偵測影片中的人臉，列出各項屬性
import boto3
import json
import sys
import time
import cv2


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

    def StartFaceDetection(self):
        response = self.rek.start_face_detection(Video={'S3Object': {'Bucket': self.bucket, 'Name': self.video}},
                                                 NotificationChannel={'RoleArn': self.roleArn, 'SNSTopicArn': self.snsTopicArn}, FaceAttributes='ALL')

        self.startJobId = response['JobId']
        print('Start Job Id: ' + self.startJobId)

    def GetFaceDetectionResults(self):
        maxResults = 10
        paginationToken = ''
        finished = False
        results = []

        while finished == False:
            response = self.rek.get_face_detection(JobId=self.startJobId,
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
                results.append({'Timestamp': faceDetection['Timestamp'], 'BoundingBox': faceDetection['Face']['BoundingBox']})
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

def DrawBoundingBox(bucket, objectName, boxes):
    tmp_filename = './input.mp4'

    s3_client = boto3.resource('s3')
    s3_client.meta.client.download_file(bucket, objectName, tmp_filename)

    cap = cv2.VideoCapture(tmp_filename)
    fps = cap.get(cv2.CAP_PROP_FPS)
    img_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    img_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc('m', 'p', '4', 'v')
    video = cv2.VideoWriter('./output03.mp4', fourcc, fps, (img_width, img_height))

    cur_idx = 0
    left = int(boxes[cur_idx]['BoundingBox']['Left'] * img_width)
    top = int(boxes[cur_idx]['BoundingBox']['Top'] * img_height)
    right = int((boxes[cur_idx]['BoundingBox']['Left'] + boxes[cur_idx]['BoundingBox']['Width']) * img_width)
    bottom = int((boxes[cur_idx]['BoundingBox']['Top'] + boxes[cur_idx]['BoundingBox']['Height']) * img_height)

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        timestamp = cap.get(cv2.CAP_PROP_POS_MSEC)
        if cur_idx < len(boxes) - 1 and timestamp >= boxes[cur_idx + 1]['Timestamp']:
            cur_idx += 1
            left = int(boxes[cur_idx]['BoundingBox']['Left'] * img_width)
            top = int(boxes[cur_idx]['BoundingBox']['Top'] * img_height)
            right = int((boxes[cur_idx]['BoundingBox']['Left'] + boxes[cur_idx]['BoundingBox']['Width']) * img_width)
            bottom = int((boxes[cur_idx]['BoundingBox']['Top'] + boxes[cur_idx]['BoundingBox']['Height']) * img_height)

        cv2.rectangle(frame, (left, top), (right, bottom), (255, 0, 0),2)
        video.write(frame)
    
    cap.release()
    video.release()

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

    analyzer.StartFaceDetection()
    if analyzer.GetSQSMessageSuccess() == True:
        # analyzer.GetFaceDetectionResults()
        bounding_boxes = analyzer.GetFaceDetectionResults()
        DrawBoundingBox(bucket,video,bounding_boxes)
        # results_json = json.dumps(results, indent=4, ensure_ascii=False)
        # with open('detection_boundingBox_results.json', 'w', encoding='utf-8') as f:
        #     f.write(results_json)
    analyzer.DeleteTopicandQueue()


if __name__ == "__main__":
    main()
