import boto3
import os
import subprocess
import math
import json

from boto3 import client as boto3_client

# Initialize the S3 client
s3 = boto3.client('s3')

# Input and output buckets
input_bucket = '129679271-input'
output_bucket = '1229679271-stage-1'

# Lambda handler
def handler(event, context):
    for record in event['Records']:
        bucket = input_bucket
        key = record['s3']['object']['key']
        download_path = f'/tmp/{key}'
        
        # Download the video to tmp directory
        download_from_s3(bucket, key, download_path)
        
        # Call the video_splitting_cmdline function
        output_dir = video_splitting_cmdline(download_path)
        
        if output_dir:  # Ensure frames were created
            
            # Upload the frames to the output_bucket
            upload_to_s3('/tmp/' + output_dir, output_dir)
            target_function_name = 'face-recognition'
            payload = {
                'bucket_name': output_bucket,
                'image_file_name': output_dir
            }
            invoke_lambda(target_function_name,payload)
        else:
            print("Frame extraction failed, no frames to upload.")

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed video.')
    }

def download_from_s3(bucket, key, download_path):
    """Download video from S3 to the local filesystem."""
    try:
        s3.download_file(bucket, key, download_path)
        # print(f"Downloaded {key} from bucket {bucket}")
    except Exception as e:
        print(f"Error downloading {key} from bucket {bucket}: {e}")

def upload_to_s3(output_dir, upload_path_base):
    """Upload frames from the local filesystem to the specified output_bucket."""

    try:
        s3.upload_file(output_dir, output_bucket, upload_path_base)
        # print(f'Uploaded {file} to {output_bucket}/{frame_upload_path}')
    except Exception as e:
        print(f"Error uploading {upload_path_base}: {e}")
        

def invoke_lambda(target_function_name,payload):
    lambda_client = boto3.client('lambda')
    
    response = lambda_client.invoke(
        FunctionName=target_function_name,
        InvocationType='Event', 
        Payload=json.dumps(payload)
    )
    

def video_splitting_cmdline(video_filename):
    """Split video into frames using FFmpeg and return the output directory if successful."""
    filename = os.path.basename(video_filename)
    outdir = os.path.splitext(filename)[0]
    output_dir = os.path.join("/tmp", outdir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    split_cmd = f'/usr/bin/ffmpeg -i {video_filename} -vframes 1 {output_dir}.jpg -y'
    try:
        subprocess.check_call(split_cmd, shell=True)
        print(f"Frames extracted to {output_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error splitting video: {e.returncode}")
        return None

    return outdir + '.jpg'
