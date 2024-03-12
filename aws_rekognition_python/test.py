import boto3

# 創建 S3 服務客戶端
s3 = boto3.client('s3')

# 存儲桶名稱
bucket_name = 'lab-faces-collection'

# 列出存儲桶中的對象
response = s3.list_objects_v2(Bucket=bucket_name)

# 檢查如果有對象被返回
if 'Contents' in response:
    # 計算對象數量
    num_objects = len(response['Contents'])
    print(f"Number of objects in {bucket_name}: {num_objects}")

    # 打印每個對象的鍵（名稱）和大小
    for obj in response['Contents']:
        print(f"Object: {obj['Key']}, Size: {obj['Size']}")
else:
    print(f"No objects found in {bucket_name}.")
