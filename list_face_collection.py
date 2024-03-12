#列出集合中所有臉部的訊息
import boto3
import json  

def list_faces_collection(collection_id):
    maxResults = 2
    faces_count = 0
    tokens = True

    session = boto3.Session(profile_name='default')
    client = session.client('rekognition')
    response = client.list_faces(CollectionId=collection_id,
                                 MaxResults=maxResults)

    print('Faces in collection ' + collection_id)

    while tokens:

        faces = response['Faces']

        for face in faces:
            print(json.dumps(face, indent=4))  
            faces_count += 1
        if 'NextToken' in response:
            nextToken = response['NextToken']
            response = client.list_faces(CollectionId=collection_id,
                                         NextToken=nextToken, MaxResults=maxResults)
        else:
            tokens = False
    return faces_count

def main():
    collection_id = 'myCollection1'
    faces_count = list_faces_collection(collection_id)
    print("faces count: " + str(faces_count))

if __name__ == "__main__":
    main()
