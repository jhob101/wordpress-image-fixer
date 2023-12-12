import datetime
import os
import sys

from PIL import Image, UnidentifiedImageError
import re
import uuid
import boto3
from botocore.exceptions import NoCredentialsError, ClientError


def get_basename(filename):
    "Extracts base part of filename without -size suffix"
    return filename.rsplit('-', 1)[0]

def get_images(s3, bucket_name):
    "Fetches list of objects in S3"
    try:
        files = s3.list_objects(Bucket=bucket_name)['Contents']
    except NoCredentialsError:
        print("No AWS credentials were found.")

    return files

def get_image_area(image):
    # Extract the size information from the filename
    size_info = re.sub(r'\.(jpg|jpeg|JPG|JPEG)$', '', image.rsplit('-', 1)[-1])
    try:
        width, height = map(int, size_info.split('x'))
        # Return the area
        return width * height
    except ValueError:
        #print(f"Invalid size info '{size_info}' in image filename '{image}'. Skipping")
        return 0

def object_exists(s3_client, bucket_name, obj_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=obj_key)
    except ClientError as e:
        # If the object does not exist
        if e.response['Error']['Code'] == '404':
            return False
    # If the object does exist
    return True

def upscale_image(s3_client, bucket_name, src_image, base_image_name, upscale_factor=2):
    # Download the image for processing
    file_name = './tmp/' + src_image.rsplit('/', 1)[-1]
    if object_exists(s3_client, bucket_name, src_image):
        s3_client.download_file(bucket_name, src_image, file_name)
    else:
        print(f"Error: {src_image} does not exist in the bucket {bucket_name}")
        return

    # Upscale the image
    try:
        with Image.open(file_name) as img:
            width, height = img.size

            # only proceed if the image is >= 600 along the longest edge
            if max(width, height) >= 600:
                new_size = (width * upscale_factor, height * upscale_factor)
                upscaled_img = img.resize(new_size, Image.BICUBIC)  # Bicubic interpolation

                if object_exists(s3_client, bucket_name, base_image_name):  # If base image exists
                    temp_file_name = './tmp/' + str(uuid.uuid4()) + '.jpg'  # Create a temp file name
                    s3_client.download_file(bucket_name, base_image_name, temp_file_name)  # Download it
                    with Image.open(temp_file_name) as existing_img:  # Open it
                        ex_width, ex_height = existing_img.size  # Get its size
                        existing_img.close()
                        if (ex_width * ex_height) > (width * height):  # If it's bigger
                            print("Existing base image size is larger. Skipping the upscaling.")
                            return

                # If it didn't return until here, either base image doesn't exist, or upscaled image is not smaller
                dir_name = os.path.dirname(base_image_name)  # Get the directory name
                os.makedirs(dir_name, exist_ok=True)  # Create the directory if it doesn't exist
                upscaled_img.save(base_image_name)

                print(f"Uploading upscaled image {base_image_name} to {bucket_name}/{base_image_name}")
                s3_client.upload_file(Filename=base_image_name, Bucket=bucket_name, Key=base_image_name)
            else:
                print("Image size is less than 600 along its longest edge. Upscaling not performed.")

    except UnidentifiedImageError:
        print(f"Unable to identify image {file_name}. It might not be an image or is unsupported/corrupt. Please check the file.")
        return

def main(bucket_name):
    original_stdout = sys.stdout  # Save a reference to the original standard output
    original_stderr = sys.stderr  # Save a reference to the original standard error output

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    output_filename = f"output_{timestamp}.txt"

    with open(output_filename, 'w') as f:
        sys.stdout = f  # Change the standard output to the file we created.
        sys.stderr = f  # Change the standard error output to the file we created.
        s3_client = boto3.client('s3')

        images = get_images(s3_client, bucket_name)
        images_data = {}
        for image in images:
            key = image['Key']
            if any(ext in key.lower() for ext in ['.jpg', '.jpeg']):
                basename = get_basename(key)
                if basename in images_data:
                    images_data[basename].append(key)
                else:
                    images_data[basename] = [key]

        for base, variants in images_data.items():

            variants_without_base = [x for x in variants if get_basename(x) == base]
            if variants_without_base:
                largest_file = max(variants_without_base, key=get_image_area)
                largest_file_ext = os.path.splitext(largest_file)[1]

                base_image_name = f"{base}{largest_file_ext}"

                if base_image_name not in variants:
                    # No base image so create one
                    largest_file_area = get_image_area(largest_file)

                    if largest_file_area != 0:
                        print(f"Upscaling image {largest_file}")
                        upscale_image(s3_client, bucket_name, largest_file, base_image_name)

            print("-----------------")
if __name__ == "__main__":
    main("image-fixer-bucket")