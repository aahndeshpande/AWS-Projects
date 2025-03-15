import os
import subprocess
import math
import boto3

aws_access_key_id = 'xyz'
aws_secret_access_key = 'xyz'
region_name = 'us-east-1'
input_bucket_name = '1229679271-input'
output_bucket_name = '1229679271-stage-1'
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                         region_name=region_name)
s3 = boto3.resource(
    service_name='s3',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)


def download_from_s3(filename, directory):
    s3_client.download_file(input_bucket_name, filename, directory)


def video_splitting_cmdline(video_filename):
    # Extract filename without extension
    filename = os.path.basename(video_filename)
    imagename = filename.split('.')[0]
    outdir = os.path.splitext(filename)[0]
    # print("split file name: " + outdir)

    # Create output directory
    outdir = os.path.join("/tmp", outdir)
    # print("output directory: " + outdir)
    os.makedirs(outdir, exist_ok=True)

    # Download video file from S3
    local_file_name = '/tmp/' + filename
    # print("input directory: " + local_file_name)
    download_from_s3(video_filename, local_file_name)

    split_cmd = 'ffmpeg -ss 0 -r 1 -i ' + local_file_name + ' -vf fps=1/2 -start_number 0 -vframes 1 ' + outdir + "/" + imagename + '.jpg -y'
    #print(split_cmd)
    try:
        subprocess.check_call(split_cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print(e.returncode)
        print(e.output)

    # fps_cmd = 'ffmpeg -i ' + video_filename + ' 2>&1 | sed -n "s/., \\(.\\) fp.*/\\1/p"'
    # fps = subprocess.check_output(fps_cmd, shell=True).decode("utf-8").rstrip("\n")
    # fps = math.ceil(float(fps))
    return outdir


def upload_folder_to_s3(local_folder, bucket_name, s3_folder=''):
    # s3_client.put_object(Bucket=bucket_name, Key=s3_folder)
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            # s3_path = os.path.join(s3_folder, os.path.relpath(local_path, local_folder))
            #print("s3_path", s3_path)
            #print("File", file)
            s3_client.upload_file(local_path, bucket_name, file)  # s3_path


def lambda_handler(event, context):
    filename = event['Records'][0]['s3']['object']['key']
    # print(filename)
    output_dir = video_splitting_cmdline(filename)
    # print("output got successfull")
    foldername = output_dir.split('/')[-1]
    # print(foldername)
    upload_folder_to_s3(output_dir, output_bucket_name, foldername + "/")