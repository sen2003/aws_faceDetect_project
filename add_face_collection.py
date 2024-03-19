# 把存在s3中的圖片加入集合中
import boto3


def add_faces_collection(bucket, photo, collection_id, external_image_id):
    session = boto3.Session(profile_name='default')
    client = session.client('rekognition')

    response = client.index_faces(CollectionId=collection_id,
                                  Image={'S3Object': {
                                      'Bucket': bucket, 'Name': photo}},
                                  ExternalImageId=external_image_id,
                                  MaxFaces=1,
                                  QualityFilter="AUTO",
                                  DetectionAttributes=['ALL'])

    print('Results for ' + photo)
    print('Faces indexed:')
    for faceRecord in response['FaceRecords']:
        print('  Face ID: ' + faceRecord['Face']['FaceId'])
        print('  Location: {}'.format(faceRecord['Face']['BoundingBox']))

    print('Faces not indexed:')
    for unindexedFace in response['UnindexedFaces']:
        print(' Location: {}'.format(
            unindexedFace['FaceDetail']['BoundingBox']))
        print(' Reasons:')
        for reason in unindexedFace['Reasons']:
            print('   ' + reason)
    return len(response['FaceRecords'])


def list_photos(bucket):
    session = boto3.Session(profile_name='default')
    s3 = session.client('s3')

    photos = []
    response = s3.list_objects_v2(Bucket=bucket)
    while response:
        for obj in response.get('Contents', []):
            photos.append(obj['Key'])

        if response['IsTruncated']:  # 檢查是否有更多的頁面
            response = s3.list_objects_v2(
                Bucket=bucket, ContinuationToken=response['NextContinuationToken'])
        else:
            break
    return photos


def main():
    bucket = 'lab-faces-collection'
    collection_id = 'myCollection1'

    photos = list_photos(bucket)  # 獲取s3中所有照片的列表
    external_image_ids = ["Huang", 'Ke', 'Shen', 'Tsou']
    total_indexed_faces = 0
    for photo, external_image_id in zip(photos, external_image_ids):
        indexed_faces_count = add_faces_collection(
            bucket, photo, collection_id, external_image_id)
        print(f"Faces indexed count for {photo}: {indexed_faces_count}")
        total_indexed_faces += indexed_faces_count

    print(f"Total faces indexed count: {total_indexed_faces}")


if __name__ == "__main__":
    main()
