# Streamlit App to use Amazon Rekognition API


import boto3
import os
from PIL import Image
import tempfile
from botocore.exceptions import ClientError
import logging
import zipfile
import io
from io import BytesIO
import base64
import shutil
import datetime
import re
from zipfile import ZipFile, ZIP_STORED
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from datetime import datetime
import threading
from multiprocessing import Pool
from googleapiclient import errors
import time
from google_auth_oauthlib.flow import InstalledAppFlow
import requests
import json
from google.oauth2.credentials import Credentials
import webbrowser
import random
from PIL import ImageOps, ExifTags
import uuid
import glob
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from googleapiclient.http import MediaFileUpload
import traceback
from urllib.parse import urlparse, parse_qs
import pyheif
import psutil

def convert_heic_to_jpeg(file_name):
    # Full path to the original HEIC file
    original_file_path = os.path.abspath(file_name)

    # Read HEIC file
    heif_file = pyheif.read(original_file_path)
    
    # Convert to PIL Image
    image = Image.frombytes(
        heif_file.mode, 
        heif_file.size, 
        heif_file.data,
        "raw",
        heif_file.mode,
        heif_file.stride,
    )

    # Save as JPEG
    jpeg_filename = f'{uuid.uuid4()}.jpg' 
    image.save(jpeg_filename, 'JPEG')

    # Read the saved jpeg file and convert it to bytes
    with open(jpeg_filename, 'rb') as img_file:
        byte_img = img_file.read()

    # Delete both the original .heic file and converted .jpg file
    os.remove(original_file_path)
    os.remove(jpeg_filename)

    return byte_img

# Define function to add faces to the collection
def add_faces_to_collection(bucket, photo, collection_id, external_image_id):
    # Set AWS details (replace with your own details)
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')


    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Initialize boto3 client for AWS Rekognition
    client = boto3.client('rekognition',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    response = client.index_faces(
        CollectionId=collection_id,
        Image={'S3Object':{'Bucket':bucket,'Name':photo}},
        ExternalImageId=external_image_id,
        MaxFaces=5,
        QualityFilter="AUTO",
        DetectionAttributes=['ALL']
    )

    return response['FaceRecords']

# Define function to list faces in the collection
def list_faces_in_collection(collection_id):
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')


    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Initialize boto3 client for AWS Rekognition
    client = boto3.client('rekognition',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    response = client.list_faces(CollectionId=collection_id)
    return [face['ExternalImageId'] for face in response['Faces']]

def make_request_with_exponential_backoff(request):
    for n in range(0, 5):
        try:
            return request.execute()
        except Exception as e:
            if isinstance(e, errors.HttpError) and e.resp.status == 403:
                time.sleep((2 ** n) + random.random())
            else:
                raise e
            print(e)
    print("Request failed after 5 retries")
    try:
        return request.execute()
    except:
        return None
    
def sanitize_name(name):
    """Sanitize names to match AWS requirements for ExternalImageId"""
    # Remove everything after a hyphen, an underscore or ".jpg"
    name = re.sub(r' -.*|_.*|\.jpg|\.JPG|\.jpeg|\.JPEG|\.png|\.PNG|\.heic|\.HEIC', '', name)
    # Keep only alphabets, spaces, and hyphens
    name = re.sub(r'[^a-zA-Z \-]', '', name)
    # Replace hyphens and spaces with underscores
    name = re.sub(r'[- ]', '_', name)
    # Replace multiple underscores with a single underscore
    name = re.sub(r'[_]+', '_', name)
    # Remove leading underscore if it exists
    if name.startswith('_'):
        name = name[1:]
    return name

def upload_file_to_s3(file, bucket, object_name):
    """
    Upload a file to an S3 bucket

    :param file: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    try:
        s3.upload_fileobj(file, bucket, object_name)
    except ClientError as e:
        logging.error('Error uploading file to S3: %s', e)
        return False
    logging.info('File uploaded successfully to bucket %s with key %s', bucket, object_name)
    return True

def find_matching_faces(photo, collection_id):
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')


    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Initialize boto3 client for AWS Rekognition
    client = boto3.client('rekognition',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Convert bytes to PIL Image
    image = Image.open(BytesIO(photo))

    # Attempt to detect faces in the photo
    faces = client.detect_faces(Image={'Bytes': photo})
    # print(f"Number of faces detected by detect_faces: {len(faces['FaceDetails'])}")
    
    # For each face, search for matches in the collection
    face_matches = []
    for i, face in enumerate(faces['FaceDetails']):
        bbox = face['BoundingBox']
        # Convert bbox to the format required by search_faces_by_image
        width, height = image.width, image.height
        left = int(bbox['Left']*width)
        top = int(bbox['Top']*height)
        face_width = int(bbox['Width']*width)
        face_height = int(bbox['Height']*height)

        # Add a buffer to the bounding box coordinates to make sure the entire face is included
        buffer = 20  # adjust this value as needed
        left = max(0, left - buffer)
        top = max(0, top - buffer)
        right = min(width, left + face_width + buffer)
        bottom = min(height, top + face_height + buffer)

        face_crop = image.crop((left, top, right, bottom))
        bytes_io = io.BytesIO()
        face_crop.save(bytes_io, format='JPEG')
        face_bytes = bytes_io.getvalue()

        try:
            response = client.search_faces_by_image(
                CollectionId=collection_id,
                Image={'Bytes': face_bytes},
                FaceMatchThreshold=70,
                MaxFaces=5
            )
            face_matches.extend([match['Face']['ExternalImageId'] for match in response['FaceMatches']])
            # print(f"Face {i+1}: matches found")
        except Exception as e:
            pass
            # print(f"Face {i+1}: error occurred - {str(e)}")

    # print(f"Number of face matches found: {len(face_matches)}")
    return face_matches

def resize_image(image, basewidth):
    img = Image.open(image)
    wpercent = (basewidth/float(img.size[0]))
    hsize = int((float(img.size[1])*float(wpercent)))
    img = img.resize((basewidth,hsize), Image.ANTIALIAS)
    return img

def process_file_wrapper(args):
    return process_file(*args)

def consolidate_labels(collection_id):
    # Dictionary to store labels
    labels_dict = {}

    # Iterate over all text files in the labels directory
    for filename in glob.glob(f'{collection_id}/labels/*.txt'):
        with open(filename, 'r') as f:
            # Read the labels from the file
            line = f.readline().strip()
            if ': ' in line:
                image_name, persons = line.split(': ')
                persons_list = persons.split(', ') if persons else []
            else:
                image_name = line[:-1]  # remove the trailing colon
                persons_list = []

            # Append the labels to the dictionary
            for person in persons_list:
                if person not in labels_dict:
                    labels_dict[person] = []
                labels_dict[person].append(image_name)

        # Delete the file after processing
        os.remove(filename)

    # Identify the 'group' key (assuming it's there)
    group_key = 'Group Photos'  # Change as needed
    group_images = labels_dict.pop(group_key, None)

    # Now, create a new consolidated file and write the 'group: images' at the top
    with open(f'{collection_id}/labels.txt', 'w') as f:
        if group_images is not None:
            f.write(f'{group_key}: {", ".join(group_images)}\n\n')

        # Write the rest of the labels
        for person, images in labels_dict.items():
            f.write(f'{person}: {", ".join(images)}\n\n')


def process_file(file, service, folder_id, person_images_dict, group_photo_threshold, collection_id, person_folder_dict, starting_memory_percent):
    try:
        # Initialize persons
        persons = []
        request = service.files().get_media(fileId=file['id'])
        # Download and process the image
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()

        if file['name'].endswith('.heic') or file['name'].endswith('.HEIC'):
            try:
                heif_file = pyheif.read(fh.getvalue())
                img = Image.frombytes(heif_file.mode, heif_file.size, heif_file.data, "raw", heif_file.mode)
                byte_arr = io.BytesIO()
                img.save(byte_arr, format='JPEG')
                img.save('converted3.jpg')
                byte_img = byte_arr.getvalue()
            except:
                img_io = io.BytesIO(fh.getvalue())
                img = resize_image(img_io, 1000)
                if img.mode != 'RGB':  # Convert to RGB if not already
                    img = img.convert('RGB')
                byte_arr = io.BytesIO()
                img.save(byte_arr, format='JPEG')
                byte_img = byte_arr.getvalue()
                
        else:  # This will cover both .jpg and .png files
            img_io = io.BytesIO(fh.getvalue())
            img = resize_image(img_io, 1000)
            if img.mode != 'RGB':  # Convert to RGB if not already
                img = img.convert('RGB')
            byte_arr = io.BytesIO()
            img.save(byte_arr, format='JPEG')
            byte_img = byte_arr.getvalue()

        detected_persons = find_matching_faces(byte_img, collection_id)

        if len(set(detected_persons)) >= group_photo_threshold:
            person_images_dict['Group Photos'].append(file['name'])
            persons = ['Group Photos']
        else:
            persons = detected_persons

        for person in set(persons):
            if person not in person_images_dict:
                person_images_dict[person] = []
            person_images_dict[person].append(file['name'])

            # Get the person's folder from the dictionary
            folder = person_folder_dict[person]

            current_year = datetime.now().year
            new_file_name = f"{person}_{current_year}_{file['name']}"
            # Check if file already exists in the destination folder
            search_response = make_request_with_exponential_backoff(service.files().list(q=f"name='{new_file_name}' and '{folder['id']}' in parents and trashed=false",
                                                                                        spaces='drive',
                                                                                        fields='files(id, name)'))

            # If file does not exist, then copy it
            if not search_response.get('files', []):
                copied_file = make_request_with_exponential_backoff(service.files().copy(fileId=file['id'], body={"name": new_file_name, "parents": [folder['id']]}))


    except Exception as e:
        print(f"{file['name']} threw an error: {e}")
        traceback.print_exc()  # This line prints the full traceback

    # Generate a unique filename using uuid library
    unique_filename = str(uuid.uuid4()) + '.txt'
    with open(f'{collection_id}/labels/{unique_filename}', 'w') as f:
        # Write the image name and persons detected to the file
        f.write(f"{file['name']}: {', '.join(set(persons))}")

    print(f"{file['name']}: {', '.join(set(persons))}")
    return file['name']

def get_memory_usage():
    memory_info = psutil.virtual_memory()
    return {
        "total_memory": memory_info.total,
        "used_memory": memory_info.used,
        "free_memory": memory_info.free,
        "percentage_used": memory_info.percent
    }

def fetch_cache_from_s3():
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    S3_BUCKET_NAME = 'li-general-task'
    CACHE_FILE_NAME = 'labeler_cache.json'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=CACHE_FILE_NAME)
        cache_content = response['Body'].read().decode('utf-8')
        return json.loads(cache_content)
    except Exception as e:
        print("Error fetching cache:", e)
        return None

def save_cache_to_s3(cache_data):
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    S3_BUCKET_NAME = 'li-general-task'
    CACHE_FILE_NAME = 'labeler_cache.json'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        s3.put_object(Body=json.dumps(cache_data, indent=4), Bucket=S3_BUCKET_NAME, Key=CACHE_FILE_NAME)
    except Exception as e:
        print("Error saving cache:", e)

def update_arguments_in_cache(args_dict):
    cache = fetch_cache_from_s3()
    if cache:
        cache['arguments'] = args_dict
        save_cache_to_s3(cache)

def append_labeled_file(file_name):
    cache = fetch_cache_from_s3()
    if cache and file_name not in cache.get('labeled', []):
        cache['labeled'].append(file_name)
        save_cache_to_s3(cache)

def load_args_from_cache():
    """
    Fetch labeler_cache.json from S3 and return arguments if present.
    """
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    S3_BUCKET_NAME = 'li-general-task'
    CACHE_FILE_NAME = 'labeler_cache.json'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=CACHE_FILE_NAME)
        cache_data = json.loads(response['Body'].read())
        return cache_data
    except Exception as e:
        print(f"Error fetching cache from S3: {e}")
        return {}
    
def update_labeled_files_in_cache(labeled_files, image_names):
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    S3_BUCKET_NAME = 'li-general-task'
    CACHE_FILE_NAME = 'labeler_cache.json'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    """
    Update the labeled_files and image_names values in the labeler_cache.json stored on S3.
    """
    try:
        # Fetch existing cache
        existing_cache = fetch_cache_from_s3()
        if not existing_cache.get('arguments'):
            print("No arguments found in cache to update.")
            return

        # Update the values
        existing_cache['arguments']['labeled_files'] = labeled_files
        existing_cache['arguments']['image_names'] = image_names

        # Put the updated cache back to S3
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=CACHE_FILE_NAME, Body=json.dumps(existing_cache))

    except Exception as e:
        print(f"Error updating labeled_files in cache: {e}")

def reset_cache_on_terminate():
    """
    Reset the labeler_cache.json on S3 to its initial state.
    """
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    S3_BUCKET_NAME = 'li-general-task'
    CACHE_FILE_NAME = 'labeler_cache.json'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        reset_data = {
            'arguments': {},
            'labeled': []
        }

        # Put the reset data back to S3
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=CACHE_FILE_NAME, Body=json.dumps(reset_data))
        
    except Exception as e:
        print(f"Error resetting cache: {e}")