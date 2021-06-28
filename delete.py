import boto3


def delete(bucket_name: str, file_name: str) -> None:
    s3resource = boto3.resource('s3')
    s3resource.Object(bucket_name, file_name).delete()
