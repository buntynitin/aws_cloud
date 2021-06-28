import boto3

def ls(bucket_name: str) -> list:
    s3resource = boto3.resource('s3')
    my_bucket = s3resource.Bucket(bucket_name)
    file_list = []
    for item in my_bucket.objects.all():
        file_list.append({
            'name': item.key,
            'size': item.size,
            'last_modified': item.last_modified.strftime("%m/%d/%Y, %H:%M:%S")
        })
    return file_list
