import boto3

def upload(bucket_name: str, file_name: str, expiration_time=30) -> str:
    s3client = boto3.client('s3')
    uri = s3client.generate_presigned_url(
        'put_object', {'Bucket': bucket_name, 'Key': file_name}, ExpiresIn=(expiration_time * 60), HttpMethod='PUT')
    return uri

