import json
import boto3
import time
import os

cw = boto3.client('logs')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    log_group = os.getenv('CLOUDWATCH_LOG_GROUP')
    log_stream = 'audit'
    token = create_log_stream(log_group, log_stream)
    
    old_version = get_previous_version(event['detail']['bucket']['name'], event['detail']['object']['key'])
    comparison = compare_files(event['detail']['bucket']['name'], event['detail']['object']['key'], event['detail']['object']['version-id'], old_version)
    process_differences(comparison, log_group, log_stream, token)
    

def compare_files(bucket, filename, newversion, oldversion):
    if oldversion == "":
        data = read_file(bucket, filename, oldversion)
        return [ {"record": row, "file": filename, "fileVersion": newversion} for row in data ]
    else:
        olddata= read_file(bucket, filename, oldversion)    
        newdata= read_file(bucket, filename, newversion)
        return [ {"record": row, "file": filename, "fileVersion": newversion} for row in newdata if row not in olddata ]


def process_differences(arrdata, log_group, log_stream, token):
    for record in arrdata:
        if record != "" and record != '\n':
            print(f"Procssing Line: {record}")
            token = write_log_event(log_group, log_stream, record, token)


def get_previous_version(bucket, filename):
    version = s3.list_object_versions(
        Bucket=bucket, 
        Prefix=filename
    )
    if len(version['Versions']) > 1:
        last_version = version['Versions'][1]['VersionId']
    else:
        last_version = ""
    
    return last_version


def read_file(bucket, filename, version):
    object = None
    
    if version == "":
        object = s3.get_object(
            Bucket=bucket,
            Key=filename
        )
    else:
        object = s3.get_object(
            Bucket=bucket,
            Key=filename,
            VersionId=version
        )
    
    with open('/tmp/temp.data', 'wb') as writer:
        for chunk in object['Body'].iter_chunks(
            chunk_size=4096
        ):
            writer.write(chunk)
            writer.close()
        
    with open('/tmp/temp.data', 'r') as reader:
            data = reader.read()

    return data.split(get_line_separator())
        
        
def create_log_stream(log_group, stream_name):
    response = has_log_stream(log_group, stream_name)
    if response['hasStream']:
        return response['token']
    else:
        cw = boto3.client('logs')
        cw.create_log_stream(
            logGroupName=log_group,
            logStreamName=stream_name
        )
        return ""
        
        
def has_log_stream(log_group, stream_name):
    response = cw.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=stream_name,
    )
    if len(response['logStreams']) > 0:
        return {
            "hasStream": True,
            "token": response['logStreams'][0]['uploadSequenceToken']
        }
    else:
        return {
            "hasStream": False
        }
        
        
def write_log_event(log_group, log_stream, event, token):
    if token == "":
        # Add a record without a token (i.e. this is the first record.)
        response = cw.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[
                {
                    "timestamp": int(round(time.time() * 1000)),
                    "message": json.dumps(event)
                }
            ]
        )
    else:
        # Add a record when specifying a Token
        response = cw.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[
                {
                    "timestamp": int(round(time.time() * 1000)),
                    "message": json.dumps(event)
                }
            ],
            sequenceToken=token
        )
    # Return the next sequence token
    return response['nextSequenceToken']
    
    
def get_line_separator():
    separator = ""
    asciis = os.getenv('FILE_SEPARATOR').split(',')
    for code in asciis:
        separator = separator + chr(int(code))
    return separator
    