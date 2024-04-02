# 列出特定臉部(照片)的詳細訊息
import boto3


def list_face_details(bucket_name, image_name):
    client = boto3.client('rekognition')

    response = client.detect_faces(
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': image_name
            }
        },
        Attributes=['ALL']
    )

    return response


bucket_name = 'lab-faces-collection'
image_name = 'img_03.jpg'
face_details = list_face_details(bucket_name, image_name)

for face in face_details['FaceDetails']:
    print("Detected face attributes:", face)
