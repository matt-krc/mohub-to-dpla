import boto3
import os
import progressbar
from datetime import datetime

def upload_s3(fpath):
    fn = fpath.split("/")[-1]
    client = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
    )
    statinfo = os.stat(fpath)
    up_progress = progressbar.progressbar.ProgressBar(maxval=statinfo.st_size)
    up_progress.start()
    def upload_progress(chunk):
        up_progress.update(up_progress.currval + chunk)
    now = datetime.now().strftime("%Y%m%d")
    print("starting upload...")
    res = client.upload_file(fpath, os.getenv('S3_BUCKET'), f"{now}/{fn}", Callback=upload_progress)
    up_progress.finish()
    print(f"finished upload to {now}/{fn}")

upload_s3("hhub_ingest.jsonl")