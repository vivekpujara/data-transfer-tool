import os
import sys
import boto3
from tqdm import tqdm
import argparse
from botocore.exceptions import ClientError

def check_conda_environment():
    """Ensure a Conda environment is active."""
    if not os.environ.get("CONDA_PREFIX"):
        print("Error: Please activate a conda environment before running this tool.")
        sys.exit(1)

def validate_s3_bucket_and_key(bucket_name, s3_object_path):
    """Validate the existence of an S3 bucket and key."""
    s3 = boto3.client("s3")
    try:
        # Check if the bucket exists
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(f"Error: Bucket '{bucket_name}' does not exist or you do not have access.")
        sys.exit(1)

    try:
        # Check if the object exists
        s3.head_object(Bucket=bucket_name, Key=s3_object_path)
        return True  # Object exists
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False  # Object does not exist
        print(f"Error: Unable to access object '{s3_object_path}' in bucket '{bucket_name}'.")
        sys.exit(1)

def upload_to_s3(file_path, bucket_name, s3_object_path, glacier=False):
    """Upload a file to AWS S3 with an option for Glacier storage class."""
    s3 = boto3.client("s3")

    # Validate file existence
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' does not exist.")
        sys.exit(1)

    # Check if the file already exists in S3
    if validate_s3_bucket_and_key(bucket_name, s3_object_path):
        print(f"Warning: A file with the key '{s3_object_path}' already exists in the bucket '{bucket_name}'.")
        response = input("Do you want to overwrite it? (yes/no): ").strip().lower()
        if response != "yes":
            print("Upload canceled by user.")
            sys.exit(0)

    # Proceed with upload
    file_size = os.path.getsize(file_path)
    storage_class = "DEEP_ARCHIVE" if glacier else "STANDARD"
    with open(file_path, "rb") as f:
        with tqdm(total=file_size, unit="B", unit_scale=True, desc="Uploading") as pbar:
            s3.upload_fileobj(
                f,
                bucket_name,
                s3_object_path,
                ExtraArgs={"StorageClass": storage_class},
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
            )

    # Validate upload
    if validate_s3_bucket_and_key(bucket_name, s3_object_path):
        print(f"File '{file_path}' successfully uploaded to '{bucket_name}:{s3_object_path}' with storage class {storage_class}.")
    else:
        print(f"Error: Upload validation failed. The file was not found in S3.")
        sys.exit(1)

def download_from_s3(bucket_name, s3_object_path, destination_path, extract=False, delete_s3_tarball=False):
    """Download a file from AWS S3 and optionally extract it."""
    s3 = boto3.client("s3")

    # Validate S3 path
    if not validate_s3_bucket_and_key(bucket_name, s3_object_path):
        print(f"Error: The object '{s3_object_path}' does not exist in bucket '{bucket_name}'.")
        sys.exit(1)

    # Check if the file already exists locally
    if os.path.exists(destination_path):
        print(f"Warning: A file already exists at the destination '{destination_path}'.")
        response = input("Do you want to overwrite it? (yes/no): ").strip().lower()
        if response != "yes":
            print("Download canceled by user.")
            sys.exit(0)

    # Proceed with download
    with open(destination_path, "wb") as f:
        with tqdm(unit="B", unit_scale=True, desc="Downloading") as pbar:
            s3.download_fileobj(
                bucket_name,
                s3_object_path,
                f,
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
            )

    # Validate download
    if os.path.exists(destination_path):
        print(f"File successfully downloaded to '{destination_path}'.")
    else:
        print(f"Error: Download validation failed. The file was not saved locally.")
        sys.exit(1)

    # Extract tarball if requested
    if extract:
        print(f"Extracting '{destination_path}'...")
        os.system(f"tar -xzvf {destination_path} -C {os.path.dirname(destination_path)}")
        print(f"Extraction complete. Data available in '{os.path.dirname(destination_path)}'.")

    # Optionally delete the S3 tarball
    if delete_s3_tarball:
        s3.delete_object(Bucket=bucket_name, Key=s3_object_path)
        print(f"Deleted tarball from S3: {bucket_name}:{s3_object_path}")

def create_tarball(source_folder, tarball_path):
    """Create a tarball of the source folder."""
    os.system(f"tar -czvf {tarball_path} -C {os.path.dirname(source_folder)} {os.path.basename(source_folder)}")
    print(f"Tarball created at: {tarball_path}")

def main():
    """Main CLI workflow."""
    check_conda_environment()

    parser = argparse.ArgumentParser(description="Transfer data between HPC clusters via AWS S3")
    subparsers = parser.add_subparsers(dest="mode", help="Mode of operation: upload or download")
    
    # Upload mode
    upload_parser = subparsers.add_parser("upload", help="Upload data to AWS S3")
    upload_parser.add_argument("--source", required=True, help="Folder to upload (from HPC Cluster #1)")
    upload_parser.add_argument("--destination", required=True, help="AWS S3 bucket and object path, e.g., mybucket:s3-path")
    upload_parser.add_argument("--temp-path", default=os.getcwd(), help="Temporary path for creating tarball (default: current working directory)")
    upload_parser.add_argument("--glacier", action="store_true", help="Store the data in Glacier Deep Archive")

    # Download mode
    download_parser = subparsers.add_parser("download", help="Download data from AWS S3")
    download_parser.add_argument("--source", required=True, help="AWS S3 bucket and tarball path, e.g., mybucket:s3-path/file.tar.gz")
    download_parser.add_argument("--destination", required=True, help="Destination folder on HPC Cluster #2")
    download_parser.add_argument("--extract", action="store_true", help="Extract tarball after download")
    download_parser.add_argument("--delete-s3-tarball", action="store_true", help="Delete the tarball from S3 after download")

    args = parser.parse_args()

    if args.mode == "upload":
        bucket_and_key = args.destination.split(":", 1)
        bucket_name = bucket_and_key[0]
        s3_object_path = bucket_and_key[1] if len(bucket_and_key) > 1 else ""
        
        # Create tarball
        tarball_name = f"{os.path.basename(args.source.rstrip('/'))}.tar.gz"
        tarball_path = os.path.join(args.temp_path, tarball_name)
        create_tarball(args.source, tarball_path)

        # Upload tarball
        upload_to_s3(tarball_path, bucket_name, s3_object_path, glacier=args.glacier)

    elif args.mode == "download":
        bucket_and_key = args.source.split(":", 1)
        bucket_name = bucket_and_key[0]
        s3_object_path = bucket_and_key[1] if len(bucket_and_key) > 1 else ""

        # Set default destination
        os.makedirs(args.destination, exist_ok=True)
        tarball_name = os.path.basename(s3_object_path)
        download_path = os.path.join(args.destination, tarball_name)

        # Download and optionally extract
        download_from_s3(bucket_name, s3_object_path, download_path, extract=args.extract, delete_s3_tarball=args.delete_s3_tarball)

    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()


"""
Sample commands and explanations:

Upload:

data-transfer upload \
    --source /path/to/source_folder \
    --destination mybucket:s3-path/data.tar.gz \
    --temp-path /scratch \
    --glacier

Explanation:
--source /path/to/source_folder: Specifies the folder on the local HPC to be uploaded.
--destination mybucket:s3-path/data.tar.gz: Specifies the S3 bucket and object path for the tarball. Here, the tarball will be uploaded to mybucket at s3-path/data.tar.gz.
--temp-path /scratch: (Optional) Sets the temporary location for creating the tarball to /scratch (useful for avoiding excessive space usage in the default location).
--glacier: (Optional) Enables Glacier Deep Archive storage class for the uploaded tarball.

---

Download:

data-transfer download \
    --source mybucket:s3-path/data.tar.gz \
    --destination /destination/folder \
    --extract \
    --delete-s3-tarball

Explanation:
--source mybucket:s3-path/data.tar.gz: Specifies the S3 bucket and object path of the tarball to download.
--destination /destination/folder: Specifies the destination folder on the local HPC where the tarball will be downloaded. If destination not specified, will download to cwd.
--extract: (Optional) Extracts the downloaded tarball into the destination folder.
--delete-s3-tarball: (Optional) Deletes the tarball from the S3 bucket after the download is complete.

---

GitHub pip installable package project folder structure:

data-transfer-tool/
├── LICENSE
├── README.md
├── setup.py
├── .gitignore
├── data_transfer_tool/
│   ├── __init__.py
│   ├── cli.py
"""