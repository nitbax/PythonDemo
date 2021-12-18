import os
import boto3

ROOT_PATH = "C:/Trainings/Python For All Screenshots"  # local folder for upload
DEST_PATH = "pyspark"
SOURCE_FILE_NAME = "s3_transfer.csv"
BUCKET_NAME = "pysparktest"
DEST_FILE_NAME = "s3check.csv"
DEST = DEST_PATH + "/" + DEST_FILE_NAME


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")

    try:
        result = s3_client.upload_file(file_name, bucket, object_name)
    except Exception as err:
        print("Error occurred wile uploading file:\n", err)
        return False
    return True


def upload_folder(root_path, bucket, dest_path):
    """Upload folder to an S3 bucket

    :param root_path: Path of folder to be uploaded
    :param bucket: Bucket to upload to
    """
    try:
        for path, subdirs, files in os.walk(root_path):
            path = path.replace("\\", "/")
            for file in files:
                result = upload_file(
                    os.path.join(path, file), bucket, dest_path + "/" + file
                )
                if result:
                    print("\nFile Uploaded Successfully 'File Name':", file)
    except Exception as err:
        print("Error occurred while uploading file:\n", err)


condition = True
while condition:
    choice = int(
        input(
            """\nSelect Option
    1. Upload Folder
    2. Upload File
    3. exit    
Enter 1,2 and 3 for selection :"""
        )
    )

    if choice == 1:
        upload_folder(ROOT_PATH, BUCKET_NAME, DEST_PATH)
    elif choice == 2:
        response = upload_file(SOURCE_FILE_NAME, BUCKET_NAME, DEST)
        if response:
            print("\nFile Uploaded Successfully 'File Name':", DEST_FILE_NAME)
    elif choice == 3:
        condition = False
    else:
        print("\nYou have entered incorrect option.")
