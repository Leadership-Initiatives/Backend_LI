from fastapi import FastAPI
from google_auth_oauthlib.flow import Flow
from fastapi.responses import RedirectResponse
import secrets
from fastapi import FastAPI, HTTPException, Request
from process import make_request_with_exponential_backoff, process_file_wrapper, consolidate_labels, get_memory_usage, save_cache_to_s3, update_arguments_in_cache, append_labeled_file, fetch_cache_from_s3, load_args_from_cache, update_labeled_files_in_cache, reset_cache_on_terminate
import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import pytz
import time

app = FastAPI()

tokens = {}  # A simple in-memory storage for tokens

# A shared state that functions can check to see if they should stop early
should_terminate = False

s3 = boto3.client('s3')
bucket_name = 'li-general-task'  # Replace with your bucket name

def save_to_s3(user_id, data):
    s3.put_object(Bucket=bucket_name, Key=f"{user_id}/credentials.json", Body=data)

def load_from_s3(user_id):
    response = s3.get_object(Bucket=bucket_name, Key=f"{user_id}/credentials.json")
    return response['Body'].read().decode()

def startup_event():
    print("\n\n\nSTARTUP\n\n\n")
    """
    On startup, check if labeled field is non-empty and if so, recall process_files.
    """
    cache_args = load_args_from_cache()
    print("cached")
    if cache_args:
        print("found cache args")
        print(cache_args)
        labeled_files_list = cache_args.get('labeled', [])
        if labeled_files_list:
            print("found a file list")
            args_for_process = cache_args.get('arguments', {})
            if args_for_process:

                folder_ids = args_for_process.get('folder_ids', [])
                destination_folder_id = args_for_process.get('destination_folder_id')
                person_images_dict = args_for_process.get('person_images_dict', {})
                group_photo_threshold = args_for_process.get('group_photo_threshold', 0)
                collection_id = args_for_process.get('collection_id')
                person_folder_dict = args_for_process.get('person_folder_dict', {})
                labeled_files = args_for_process.get('labeled_files', 0)
                total_files = args_for_process.get('total_files', 0)
                cache = args_for_process.get('cache', {})
                creds = args_for_process.get('creds', {})
                image_names = args_for_process.get('image_names', [])
                print("called function")
                process_files(folder_ids, destination_folder_id, person_images_dict, group_photo_threshold, collection_id, person_folder_dict, labeled_files, total_files, cache, creds, image_names)


@app.get("/auth")
def auth(user_id: str):
    flow = Flow.from_client_secrets_file(
        'credentials.json',
        scopes=['https://www.googleapis.com/auth/drive'],
        redirect_uri='https://yourapp.com/callback'  # Update your redirect URI
    )
    flow.state = secrets.token_hex(16)
    authorization_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
    # Serialize flow to store in S3 (example uses pickle, adjust based on your security practices)
    import pickle
    serialized_flow = pickle.dumps(flow)
    save_to_s3(user_id, serialized_flow)
    return {"authorization_url": authorization_url}

@app.get("/callback")
async def callback(code: str, state: str, user_id: str):
    try:
        serialized_flow = load_from_s3(user_id)
        import pickle
        flow = pickle.loads(serialized_flow)
        flow.fetch_token(code=code)
        credentials = flow.credentials
        # Optionally, save credentials to S3 or proceed with the token
        return "Authentication successful. Please close this window."
    except Exception as e:
        raise HTTPException(status_code=500, detail="Network issues, please refresh the page.")

@app.get("/token/{user_id}")
async def get_token(user_id: str):
    # Retrieve serialized credentials from S3
    serialized_creds = load_from_s3(user_id)
    import pickle
    credentials = pickle.loads(serialized_creds)
    return {"token": credentials.token}

@app.get("/status")
async def get_status():
    try:
        with open("status.txt", "r") as f:
            status_content = f.read()
        return {"status": "success", "content": status_content}
    except FileNotFoundError:
        # This will return a 404 response with a custom message when the file is not found
        raise HTTPException(status_code=404, detail="status.txt not found.")
    except Exception as e:
        # This will return a 500 response with a custom error message for other exceptions
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def get_status():
    with open('terminate.txt', 'w') as f:
        f.write("initial")
    with open('setup.txt', 'r') as f:
        setup = f.read()
    if setup == "initial":
        with open('setup.txt', 'w') as f:
            f.write("setup initiatied")
        startup_event()
    
@app.post("/process")
async def process_endpoint(request: Request):
    try:
        reset_cache_on_terminate()
        # Write the status message to status.txt
        with open('terminate.txt', 'w') as f:
            f.write(f"initial")
        data = await request.json()
        

        folder_ids = data['folder_ids']
        destination_folder_id = data['destination_folder_id']
        person_images_dict = data['person_images_dict']
        group_photo_threshold = data['group_photo_threshold']
        collection_id = data['collection_id']
        person_folder_dict = data['person_folder_dict']
        labeled_files = data['labeled_files']
        total_files = data['total_files']
        cache = data['cache']
        creds = data['creds']
        image_names = data['image_names']
        
        result = process_files(folder_ids, destination_folder_id, person_images_dict, group_photo_threshold, collection_id, person_folder_dict, labeled_files, total_files, cache, creds, image_names)
        return {"status": "success", "result": result}

    except Exception as e:
        print(f"Error processing files: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing files: {e}")

def process_files(folder_ids, destination_folder_id, person_images_dict, group_photo_threshold, collection_id, person_folder_dict, labeled_files, total_files, cache, creds, image_names):
    # Store arguments in the S3 cache
    args_dict = {
        'folder_ids': folder_ids,
        'destination_folder_id': destination_folder_id,
        'person_images_dict': person_images_dict,
        'group_photo_threshold': group_photo_threshold,
        'collection_id': collection_id,
        'person_folder_dict': person_folder_dict,
        'labeled_files': labeled_files,
        'total_files': total_files,
        'cache': cache,
        'creds': creds,
        'image_names': image_names
    }
    update_arguments_in_cache(args_dict)
    # Fetch cache to get labeled files list
    s3_cache = fetch_cache_from_s3()
    labeled_files_names = s3_cache.get('labeled', []) if s3_cache else []

    original_image_count = len(image_names)
    
    # Convert the current UTC time to EST
    est = pytz.timezone('US/Eastern')
    current_time = datetime.now(pytz.utc).astimezone(est).strftime('%m/%d/%y %I:%M%p EST')
    
    print(f"\n\n\n({collection_id}: {current_time}) STARTING BATCH JOB: {len(image_names)} PHOTOS\n\n\n\n")

    # Write the status message to status.txt
    with open('status.txt', 'w') as f:
        f.write(f"({collection_id}: {current_time}): Photo processing ({len(image_names)} images) in progress! Do not submit another job.")

    if not os.path.exists(f'{collection_id}/labels'):
                        os.makedirs(f'{collection_id}/labels')

    CLIENT_SECRET_FILE = 'credentials.json'
    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive']

    with open(CLIENT_SECRET_FILE, 'r') as f:
        client_info = json.load(f)['web']

    creds_dict = creds
    creds_dict['client_id'] = client_info['client_id']
    creds_dict['client_secret'] = client_info['client_secret']
    creds_dict['refresh_token'] = creds_dict.get('_refresh_token')
    # Create Credentials from creds_dict##
    creds = Credentials.from_authorized_user_info(creds_dict)

    # Call the Drive v3 API
    service = build(API_NAME, API_VERSION, credentials=creds)

    flag = False
    for folder_id in folder_ids:
        try:
            page_token = None

            while True:
                response = make_request_with_exponential_backoff(service.files().list(q=f"'{folder_id}' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'",
                                                                                    spaces='drive', 
                                                                                    fields='nextPageToken, files(id, name)',
                                                                                    pageToken=page_token,
                                                                                    pageSize=1000))
                items = response.get('files', [])
                # Exclude already labeled files when creating the arguments #
                new_items = [file for file in items if file['name'] not in labeled_files_names]
                items = new_items
    
                starting_memory_percent = (get_memory_usage())['percentage_used']
                arguments = [(file, service, destination_folder_id, person_images_dict, group_photo_threshold, collection_id, person_folder_dict, starting_memory_percent,) for file in items]
                with ProcessPoolExecutor(max_workers=5) as executor:
                    futures = {executor.submit(process_file_wrapper, arg): arg for arg in arguments}
                    for future in as_completed(futures):
                        try:

                            # Convert the current UTC time to EST
                            est = pytz.timezone('US/Eastern')
                            current_time = datetime.now(pytz.utc).astimezone(est).strftime('%m/%d/%y %I:%M%p EST')
                            # Write the status message to status.txt
                            with open('terminate.txt', 'r') as f:
                                content = f.read()
                            print(content)
                            if content == "terminate":
                                print(content)
                                print(f"\n\n\n({collection_id}: {current_time}) : JOB TERMINATED\n\n\n")
                                # Write the status message to status.txt
                                with open('status.txt', 'w') as f:
                                    f.write(f"({collection_id}: {current_time}) : JOB TERMINATED")
                                return
                            result = future.result()  # replace with appropriate handling if process_file_wrapper returns something
                            cache['labeled_files'].append(result)
                            labeled_files += 1
                            cache['file_progress'] += 1
                            print(f"{labeled_files}/{total_files}")
                            with open('status.txt', 'w') as f:
                                f.write(f"({collection_id}: {current_time}): Photo processing ({labeled_files}/{total_files} images) in progress! Do not submit another job.")
                            flag = True
                            image_names.remove(result)
                            # Update the labeled_files and image_names in the cache
                            update_labeled_files_in_cache(labeled_files, image_names)
                            append_labeled_file(result)
                            # progress_report.text(f"Labeling progress: {max(labeled_files, cache['file_progress'])}/{total_files} ({round(remaining_time, 1)} minutes remaining)")
                        except Exception as e:
                            print(e)
                            # Update the labeled_files and image_names in the cache
                            update_labeled_files_in_cache(labeled_files, image_names)
                            append_labeled_file(result)
                            labeled_files += 1
                            cache['file_progress'] += 1
                            remaining_time = (total_files - max(labeled_files, cache['file_progress'])) * (1/30)
                            flag = True

                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break
            # Check the length of image_names
            if len(image_names) == 0:
                status_message = f"({collection_id}: {current_time}) FINAL: All {original_image_count} images have been successfully labeled!\n Folder IDs submitted: {str(folder_ids)}"
            else:
                status_message = f"({collection_id}: {current_time}) FINAL: The following images have not been labeled: " + str(image_names)

            with open('terminate.txt', 'r') as f:
                text = f.read()

            if text == "initial":
                with open('terminate.txt', 'w') as f:
                    f.write(f"proceed")
            else:
                # Print the status message
                print("\n\n\n" + status_message + "\n\n\n")

                # Write the status message to status.txt
                with open('status.txt', 'w') as f:
                    f.write(status_message)


        except Exception as e:
            print(e)
            print(image_names)

    consolidate_labels(collection_id)
    if not flag:
        # progress_report.text("")
        # progress_report_folder.text("")##
        pass

    # return cache #
    return image_names

@app.get("/terminate")
def terminate_all():
    # Write the status message to terminate.txt
    with open('terminate.txt', 'w') as f:
        f.write("terminate")

    # Reset the labeler_cache.json on S3
    reset_cache_on_terminate()

    return {"status": "All jobs have been terminated and cache has been reset!"}


