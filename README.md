# Data Transfer Tool

A Python CLI tool to transfer data between HPC clusters using AWS S3.

## Installation

### From GitHub (for the latest version):
To install the tool directly from GitHub, use the following command:
```bash
pip install git+https://github.com/vivekpujara/data-transfer-tool.git
```

### Sample commands and explanations:

### Upload:
```bash
data-transfer upload \
    --source /path/to/source_folder \
    --destination mybucket:s3-path/data.tar.gz \
    --temp-path /scratch \
    --glacier
```
Explanation:
- `--source /path/to/source_folder`: Specifies the folder on the local HPC to be uploaded.
- `--destination mybucket:s3-path/data.tar.gz`: Specifies the S3 bucket and object path for the tarball. Here, the tarball will be uploaded to mybucket at s3-path/data.tar.gz.
- `--temp-path /scratch`: (Optional) Sets the temporary location for creating the tarball to /scratch (useful for avoiding excessive space usage in the default location).
- `--glacier`: (Optional) Enables Glacier Deep Archive storage class for the uploaded tarball.

---

### Download:
```bash
data-transfer download \
    --source mybucket:s3-path/data.tar.gz \
    --destination /destination/folder \
    --extract \
    --delete-s3-tarball
```
Explanation:
- `--source mybucket:s3-path/data.tar.gz`: Specifies the S3 bucket and object path of the tarball to download.
- `--destination /destination/folder`: Specifies the destination folder on the local HPC where the tarball will be downloaded. If destination not specified, will download to cwd.
- `--extract`: (Optional) Extracts the downloaded tarball into the destination folder.
- `--delete-s3-tarball`: (Optional) Deletes the tarball from the S3 bucket after the download is complete.

### License
This project is licensed under the MIT License.