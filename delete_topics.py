import boto3

sns = boto3.client('sns')

response = sns.list_topics()

if 'Topics' in response:
    topics = response['Topics']
    for topic in topics:
        topic_arn = topic['TopicArn']
        print(f"Deleting topic:{topic_arn}")
        sns.delete_topic(TopicArn=topic_arn)
else:
    print("No topics found.")
