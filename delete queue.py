import boto3

sqs = boto3.client('sqs')

response = sqs.list_queues()

if 'QueueUrls' in response:
    queue_urls = response['QueueUrls']
    for queue_url in queue_urls:
        print("Deleting queue:", queue_url)
        sqs.delete_queue(QueueUrl=queue_url)
else:
    print("No queues found.")
