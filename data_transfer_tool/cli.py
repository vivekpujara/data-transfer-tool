import os
import sys
import boto3
import argparse
from tqdm import tqdm
from botocore.exceptions import ClientError
import subprocess

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
    """Create a tarball of the source folder with resumable capability using the tar -czvf CLI command.
    
    This function uses the system's tar command to create a compressed tarball.
    If a tarball already exists, it lists the archived files (using tar -tzf) and computes
    the missing files, then creates a temporary tarball of those files and concatenates it
    to the existing tarball (tar concatenation of gzip files is supported by GNU tar).
    
    A tqdm progress bar is used to display progress for the tarballing process.
    The progress file is created in the same temp directory as the tarball,
    and its name includes the basename of the folder being uploaded.
    """
    # Resolve absolute paths and determine names
    source_folder = os.path.abspath(source_folder)
    source_basename = os.path.basename(source_folder.rstrip("/"))
    source_parent = os.path.dirname(source_folder.rstrip("/"))
    progress_file = os.path.join(os.path.dirname(tarball_path),
                                 f"{source_basename}.filelist.txt")
    
    if not os.path.exists(tarball_path):
        # Create tarball from scratch.
        # Count total number of files in the source folder.
        total_files = 0
        for root, _, files in os.walk(source_folder):
            total_files += len(files)
        # Build the tar command.
        command = f"tar -czvf {tarball_path} -C {source_parent} {source_basename}"
        print(f"Running tar command: {command}")
        pbar = tqdm(total=total_files, unit="files", desc="Tarballing")
        archived_list = []
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        for line in process.stdout:
            stripped = line.strip()
            if stripped:
                archived_list.append(stripped)
                pbar.update(1)
        process.wait()
        pbar.close()
        if process.returncode != 0:
            print("Error: Tarball creation failed.")
            sys.exit(1)
        # Write progress file with the names of all archived files.
        with open(progress_file, 'w') as f:
            for archived in archived_list:
                f.write(archived + "\n")
            f.write("Complete\n")
        print(f"\nTarball created successfully: {tarball_path}")
        print(f"Progress saved to: {progress_file} (retained after completion)")
    else:
        # Tarball exists, so check for missing files using tar -tzf.
        print("Existing tarball found. Checking for missing files to resume...")
        # List files in existing tarball (paths are relative to source_parent)
        cmd_list = f"tar -tzf {tarball_path}"
        tar_list_output = os.popen(cmd_list).read().splitlines()
        archived_files = set(tar_list_output)
        
        # Build set of all files in the source folder relative to source_parent.
        all_files = set()
        for root, _, files in os.walk(source_folder):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, source_parent)
                all_files.add(rel_path)
                
        missing_files = all_files - archived_files
        if not missing_files:
            print("All files have already been archived.")
        else:
            print(f"Resuming tarball creation. {len(missing_files)} files remaining.")
            missing_files_list = list(missing_files)
            missing_files_str = " ".join([f"'{f}'" for f in missing_files_list])
            temp_tarball = tarball_path + ".temp"
            command = f"tar -czvf {temp_tarball} -C {source_parent} {missing_files_str}"
            print(f"Running tar command for missing files: {command}")
            pbar = tqdm(total=len(missing_files_list), unit="files", desc="Resuming Tarballing")
            temp_archived = []
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
            for line in process.stdout:
                stripped = line.strip()
                if stripped:
                    temp_archived.append(stripped)
                    pbar.update(1)
            process.wait()
            pbar.close()
            if process.returncode != 0:
                print("Error: Tarball creation for missing files failed.")
                sys.exit(1)
            # Concatenate the existing tarball and the temporary tarball.
            new_tarball = tarball_path + ".new"
            cat_command = f"cat {tarball_path} {temp_tarball} > {new_tarball}"
            print(f"Concatenating tarballs with command: {cat_command}")
            ret = os.system(cat_command)
            if ret != 0:
                print("Error: Concatenating tarballs failed.")
                sys.exit(1)
            os.replace(new_tarball, tarball_path)
            os.remove(temp_tarball)
            print("Tarball resumed and updated successfully.")
            # Update progress file with the union of archived files.
            all_archived = archived_files.union(set(temp_archived))
            with open(progress_file, 'w') as f:
                for file in sorted(all_archived):
                    f.write(file + "\n")
                f.write("Resumed and complete\n")
            print(f"Progress saved to: {progress_file} (retained after completion)")

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
