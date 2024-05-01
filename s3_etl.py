import boto3
from datetime import datetime
print('empatica-etl-lambda started')
# Define your AWS credentials and region
#source_bucket_name = 'empatica-us-east-1-prod-data'
#aws_source_prefix = 'v2/566/' # format b/100/
participants =[
    "1/1"
]
# Define your AWS credentials and region
source_bucket_name = 'empatica-us-east-1-prod-data'
#today = datetime.today().strftime('%Y-%m-%d')
today = '2024-02-15'
aws_source_prefix = f'v2/566/<participant>/participant_data/{today}' # format b/100/
aws_source_access_key_id = ''
aws_source_secret_access_key = ''
aws_source_region = 'us-east-1'

destination_bucket_name = 'test-empatica-etl-data'  # Your bucket name
aws_destination_access_key_id = ''
aws_destination_secret_access_key = ''
aws_destination_region ='eu-west-1'

print('empatica-etl-lambda init boto3')
# Create a Boto3 client for S3
s3_client_source = boto3.client('s3',
                         region_name=aws_source_region,
                         aws_access_key_id=aws_source_access_key_id,
                         aws_secret_access_key=aws_source_secret_access_key)

print('empatica-etl-lambda source boto3 is ready')

s3_client_destination = boto3.client('s3',
                         region_name=aws_destination_region,
                         aws_access_key_id=aws_destination_access_key_id,
                         aws_secret_access_key=aws_destination_secret_access_key)

print('empatica-etl-lambda destination boto3 is ready')

# Function to copy new lines from source CSV file to destination CSV file
def copy_new_lines(source_bucket, source_key, destination_bucket, destination_key):
    # Get existing content of destination file
    try:
        response = s3_client_destination.get_object(Bucket=destination_bucket, Key=destination_key)
        existing_content = response['Body'].read().decode('utf-8').split('\n')
    except Exception as err:
        existing_content = []

    # Get new content from source file
    try:
        response = s3_client_source.get_object(Bucket=source_bucket, Key=source_key)
        new_content = response['Body'].read().decode('utf-8').split('\n')
    except Exception as err:
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
        
        return len(new_lines)
    else:
        print(f"No new lines found in {source_key}")
        return 0

def copy_file(source_bucket, source_key, destination_bucket, destination_key):
    try:
        s3_client_destination.head_object(Bucket=destination_bucket, Key=destination_key)
        print(f"File {destination_key} already exists in the destination bucket.")
        return 0
    except Exception as err:
        # The file doesn't exist in the destination bucket, so copy it
        response = s3_client_source.get_object(Bucket=source_bucket, Key=source_key)
        new_content = response['Body'].read()
        s3_client_destination.put_object(Body=new_content, Bucket=destination_bucket, Key=destination_key)
        #copy_source = {'Bucket': source_bucket, 'Key': source_key}
        #s3_client_destination.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        print(f"File {destination_key} copied to the destination bucket.")
        return 1

def update_db(participant):
    # Retrieve environment variables
    db_host = os.environ['DBHOST']
    db_user = os.environ['DBUSER']
    db_password = os.environ['DBPWD']
    db_name = os.environ['DBNAME']

    # Construct the connection string
    conn_string = f"host='{db_host}' dbname='{db_name}' user='{db_user}' password='{db_password}'"
    current_timestamp = datetime.now()

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        update_query = "UPDATE experiment.participants SET empatica_last_update = %s WHERE empatica_id = %s"
        values = (current_timestamp, participant) 

        # Execute the update query
        cursor.execute(update_query, values)
        conn.commit()

        # Close cursor and connection
        cursor.close()
        conn.close()

        print("participant db updated")
        
    except Exception as e:
        print("Error updating record:", e)

print('empatica-etl-lambda before participants loop')        

for participant in participants:
    aws_source_prefix =aws_source_prefix.replace('<participant>',participant)
    # List objects in the source bucket
    print(f"fetch participant {participant} data, query={aws_source_prefix}")
    response = s3_client_source.list_objects_v2(Bucket=source_bucket_name,Prefix=aws_source_prefix)
    print(response)
    
    if 'Contents' in response:
        print(f"content exists: {response['Contents']}")
        # Iterate over objects in the source bucket
        for obj in response['Contents']:
            object_key = obj['Key']
            
            res = 0
            # if object_key.endswith('.csv'):
            # Check if the file already exists in the destination bucket
            try:
                print("check if file exists in destination")
                s3_client_destination.head_object(Bucket=destination_bucket_name, Key=object_key)
                print("file in destination, so copy new lines")
                res = copy_new_lines(source_bucket_name, object_key, destination_bucket_name, object_key)
            except Exception as err:
                # The file doesn't exist in the destination bucket, so copy it
                print("file not in destination. copy new file")
                res = copy_file(source_bucket_name, object_key, destination_bucket_name, object_key)
            
            if res > 0 :
                print("update db")
                update_db(participant)
    else:
        print("No objects found in the source bucket.")

print('empatica-etl-lambda completed')

