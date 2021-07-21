from flask import Flask, send_file, request
import logging
import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
import tempfile
from datetime import datetime
import os
from io import BytesIO
from azure.storage.fileshare import ShareClient, ShareFileClient
from azure.storage.fileshare.aio import ShareClient as ShareClientAsync
import shutil
import response_lib

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
SIZE_LIMIT = 273741824

protocol = "https"
suffix = "core.windows.net"
storage_account_name = "feasstorage1"
storage_key = "bzbrQbMbpGp0SbDURB8eFYpV4cOZkNqOeoBciyRLXrdjMIa3z6H6z6aMd31RyzZXTojOeU/AoILNPSvIOrF+zQ=="
connection_string = "DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2};EndpointSuffix={3}".format(
    protocol, storage_account_name, storage_key, suffix
)
share_name = "file-share-1"
share_client = ShareClient.from_connection_string(
    conn_str=connection_string, share_name="file-share-1"
)

@app.route("/")
def hello_world():
    return "hello"

@app.route("/download-folder", methods=["POST"])
def download_zip():
    json_data = request.json
    directory_path = json_data["path"]
    print("START Dowload: ", datetime.utcnow())
    directory_name = tempfile.mkdtemp()
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.SelectorEventLoop())
        loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=10):
        loop = asyncio.get_event_loop()
        try:           
            loop.run_until_complete(download_files_async(directory_path, directory_name))
        except TypeError as e:
            print(e)
            return response_lib.error_response(400, 11)
        except ValueError as e:
            print(e)
            return response_lib.error_response(400, 20)
    shutil.make_archive(directory_name+"/download_file", "zip", directory_name+"/"+directory_path)
    with open(directory_name +"/download_file.zip", 'rb') as f:
        file_download = BytesIO(f.read())
    shutil.rmtree(directory_name)
    print("END Dowload: ", datetime.utcnow())
    return send_file(file_download, attachment_filename="download.zip", as_attachment=True)

def list_directories_and_files(directory_path: str):
    zip_size = 0
    try:
        list_file_in_path = list(share_client.list_directories_and_files(directory_path))
    except:       
        raise TypeError()
    for item in list_file_in_path:
        if item["is_directory"]:
            yield from list_directories_and_files(directory_path + "/" + item["name"])
        else:
            yield directory_path + "/" + item["name"]
            zip_size += item["size"]
            # Check if total size before zipping is larger than 
            if int(zip_size) >= SIZE_LIMIT:
                raise ValueError()

async def download_files_async(directory_path: str, dest_path: str):
    share_client = ShareClientAsync.from_connection_string(
        connection_string, share_name
    )
    async with share_client:
        tasks = []
        try:
            for path in list_directories_and_files(directory_path):
                # Get Azure file share client.
                file_client = share_client.get_file_client(file_path=path)
                # Get file name
                file_name = path.split("/")[-1]
                # Get file path
                file_path_tmp = (
                    dest_path + "/" + "/".join([x for x in path.split("/")[:-1]])
                )
                # Create sub folder
                os.makedirs(file_path_tmp, exist_ok=True)
                # Execute dowload file
                task = asyncio.ensure_future(
                    __dowload_file_async(file_client, file_path_tmp + "/" + file_name)
                )
                tasks.append(task)
            await asyncio.gather(*tasks)
        finally:
            # Close share client.
            await share_client.close()


async def __dowload_file_async(file_client: ShareFileClient, file_path: str):
    with open(file_path, "wb") as file_data:
        stream = await file_client.download_file()
        file_data.write(await stream.readall())

if __name__ == '__main__':
   app.run()