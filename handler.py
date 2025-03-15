import boto3
import os
import cv2
from PIL import Image
from facenet_pytorch import MTCNN, InceptionResnetV1
import torch
import json

input_bucket = '1229679271-stage-1'
output_bucket = '1229679271-output'

session = boto3.Session(
    
    aws_access_key_id = 'xyz'
    aws_secret_access_key = 'xyz'
)
s3_client = session.client('s3', region_name='us-east-1')

os.environ['TORCH_HOME'] = '/tmp/'
mtcnn = MTCNN(image_size=240, margin=0, min_face_size=20)  # initializing mtcnn for face detection
resnet = InceptionResnetV1(pretrained='vggface2').eval()  # initializing resnet for face img to embeding conversion

def face_recognition_function(key_path):
    # Face extraction

    img = cv2.imread(key_path, cv2.IMREAD_COLOR)
    boxes, _ = mtcnn.detect(img)

    # Face recognition
    key = os.path.splitext(os.path.basename(key_path))[0].split(".")[0]
    img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
    face, prob = mtcnn(img, return_prob=True, save_path=None)
    saved_data = torch.load('data.pt')  # loading data.pt file
    if face != None:
        emb = resnet(face.unsqueeze(0)).detach()  # detech is to make required gradient false
        embedding_list = saved_data[0]  # getting embedding data
        name_list = saved_data[1]  # getting list of names
        dist_list = []  # list of matched distances, minimum distance is used to identify the person
        for idx, emb_db in enumerate(embedding_list):
            dist = torch.dist(emb, emb_db).item()
            dist_list.append(dist)
        idx_min = dist_list.index(min(dist_list))

        # Save the result name in a file
        with open("/tmp/" + key + ".txt", 'w+') as f:
            f.write(name_list[idx_min])
        return (key + ".txt", name_list[idx_min])
    else:
        print(f"No face is detected")

def download_from_input_bucket(filename):
    try:
        local_path = f'/tmp/{filename.split("/")[-1]}'
        s3_client.download_file(input_bucket, filename, local_path)
        return local_path

    except Exception as err:
        print("Error: ", err)
        return None

def upload_to_output_bucket(outputFile):
    s3_client.upload_file('/tmp/' + outputFile, output_bucket, outputFile)
        
def handler(event, context):
    filename = event["Records"][0]["s3"]["object"]["key"]
    local_path = download_from_input_bucket(filename)
    outputFile, name = face_recognition_function(local_path)
    upload_to_output_bucket(outputFile)
    return {
        'statusCode': 200,
        'body': json.dumps('Response from Face_Recognition'),
        'resp': name
	}