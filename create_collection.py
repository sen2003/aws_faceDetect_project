#創建集合
import boto3


def create_collection(collection_id):
    session = boto3.Session(profile_name='default')
    client = session.client('rekognition')

    # Create a collection
    print('Creating collection:' + collection_id)
    response = client.create_collection(CollectionId=collection_id)
    print('Collection ARN: ' + response['CollectionArn'])
    print('Status code: ' + str(response['StatusCode']))
    print(f"{collection_id} successfully created")


def main():
    collection_id = "myCollection1"
    create_collection(collection_id)


if __name__ == "__main__":
    main()