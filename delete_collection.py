import boto3
from botocore.exceptions import ClientError

def delete_collection(collection_id):

    print('Attempting to delete collection ' + collection_id)
    session = boto3.Session(profile_name='default')
    client = session.client('rekognition')

    status_code = 0

    try:
        response = client.delete_collection(CollectionId=collection_id)
        status_code = response['StatusCode']

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('The collection ' + collection_id + ' was not found ')
        else:
            print('Error other than Not Found occurred: ' + e.response['Error']['Message'])
        status_code = e.response['ResponseMetadata']['HTTPStatusCode']
    return (status_code)

def main():

    collection_id = 'myCollection1'
    status_code = str(delete_collection(collection_id))
    if(status_code=='200'):
        print(f"Status code: {str(status_code)} , {collection_id} successfully deleted")
    else:
        print(f"Status code: {str(status_code)} , {collection_id} has been deleted")
if __name__ == "__main__":
    main()

