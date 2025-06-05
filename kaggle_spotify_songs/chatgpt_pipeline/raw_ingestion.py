import requests 
from datetime import datetime
from zipfile import ZipFile
from pathlib import Path

#configs
url = "https://www.kaggle.com/api/v1/datasets/download/devdope/900k-spotify"
today = datetime.today().date()
filename = f"spotify_raw_download_{today}"
zip_path = f"chatgpt_pipeline/data/zip/{filename}.zip"
unzip_path = f'chatgpt_pipeline/data/unzip/'


def create_path_directories(file_location):
    """
    Given a full path to a fdescile (zip,text,etc). 
    WARNING: Must contain a final file path or will accidentally remove ending dir
    """
    if '.' in file_location:
        download_dir = '/'.join(file_location.split('/')[:-1])
    else:
        download_dir = file_location
    dir_path = Path(download_dir)
    if not dir_path.exists():
        dir_path.mkdir(parents=True)

def spotify_kaggle_download(download_path):
    """
    Downloads the spotify dataset from Kaggle:
    https://www.kaggle.com/datasets/devdope/900k-spotify
    """
    response = requests.get(url, stream=True)
    create_path_directories(download_path)
    if response.status_code == 200:
        with open(download_path, "wb") as file: # 'wb' for writing binary
            for chunk in response.iter_content(chunk_size=8192):  # 8KB chunk size
                if chunk: # Filter out any incomplete chunk
                    file.write(chunk)
        print("File downloaded successfully")
    else:
        print(f"Error: {response.status_code}")

def open_zip_file(zip_path, download_path=None):
    """
    Extracts zip files to the current path
    """
    create_path_directories(download_path)
    with ZipFile(zip_path, 'r') as zip_obj:
        zip_obj.printdir()
        zip_obj.extractall(path=download_path)

if __name__ == '__main__':
    if not Path(zip_path).exists():
        spotify_kaggle_download(zip_path)
    if not Path(unzip_path).exists():
        open_zip_file(zip_path, unzip_path)

    import json
    file_loc = "/Users/jasminewilliams/git/chatgpt_exam/chatgpt_pipeline/data/unzip/900k Definitive Spotify Dataset.json"
    
    with open(file_loc, 'r') as f:
        for i, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                if data["song"] and data["Artist(s)"] and \
                data['song'].lower() == "runaway girl" and 'drake' in  data["Artist(s)"].lower():
                    print(data)
            except json.JSONDecodeError as e:
                print(f"Malformed JSON on line {i}: {e}")