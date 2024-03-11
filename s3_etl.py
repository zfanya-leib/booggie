import boto3

# Define your AWS credentials and region
source_bucket_name = ''
aws_source_prefix = '' # format b/100/
aws_source_access_key_id = ''
aws_source_secret_access_key = ''
aws_source_region = ''

destination_bucket_name = 'destination-bucket-name'  # Your bucket name
aws_destination_access_key_id = ''
aws_destination_secret_access_key = ''
aws_destination_region =''

# Create a Boto3 client for S3
s3_client_source = boto3.client('s3',
                         region_name=aws_source_region,
                         aws_access_key_id=aws_source_access_key_id,
                         aws_secret_access_key=aws_source_secret_access_key)

s3_client_destination = boto3.client('s3',
                         region_name=aws_destination_region,
                         aws_access_key_id=aws_destination_access_key_id,
                         aws_secret_access_key=aws_destination_secret_access_key)

# Function to copy new lines from source CSV file to destination CSV file
def copy_new_lines(source_bucket, source_key, destination_bucket, destination_key):
    # Get existing content of destination file
    try:
        response = s3_client_destination.get_object(Bucket=destination_bucket, Key=destination_key)
        existing_content = response['Body'].read().decode('utf-8').split('\n')
    except s3_client_destination.exceptions.NoSuchKey:
        existing_content = []

    # Get new content from source file
    try:
        response = s3_client_source.get_object(Bucket=source_bucket, Key=source_key)
        new_content = response['Body'].read().decode('utf-8').split('\n')
    except s3_client_source.exceptions.NoSuchKey:
        print(f"File {source_key} not found in the source bucket.")
        return

    # Append new lines to existing content
    new_lines = [line for line in new_content if line not in existing_content]
    if new_lines:
        # Append new lines to the destination file
        new_data = '\n'.join(new_lines)
        new_data_bytes = new_data.encode('utf-8')
        s3_client_destination.put_object(Bucket=destination_bucket, Key=destination_key, Body=new_data_bytes)
        print(f"Copied {len(new_lines)} new lines to {destination_key}")
    else:
        print(f"No new lines found in {source_key}")

def copy_file(source_bucket, source_key, destination_bucket, destination_key):
    try:
        s3_client_destination.head_object(Bucket=destination_bucket, Key=destination_key)
        print(f"File {destination_key} already exists in the destination bucket.")
    except s3_client_destination.exceptions.NoSuchKey:
        # The file doesn't exist in the destination bucket, so copy it
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3_client_source.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        print(f"File {destination_key} copied to the destination bucket.")


# List objects in the source bucket
response = s3_client_source.list_objects_v2(Bucket=source_bucket_name,Prefix=aws_source_prefix)

if 'Contents' in response:
    # Iterate over objects in the source bucket
    for obj in response['Contents']:
        object_key = obj['Key']
        # Check if the file already exists in the destination bucket
        try:
            s3_client_destination.head_object(Bucket=destination_bucket_name, Key=object_key)
            copy_new_lines(source_bucket_name, object_key, destination_bucket_name, object_key)
        except s3_client_destination.exceptions.NoSuchKey:
            # The file doesn't exist in the destination bucket, so copy it
            copy_file(source_bucket_name, object_key, destination_bucket_name, object_key)
else:
    print("No objects found in the source bucket.")
