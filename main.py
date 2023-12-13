import datetime
import os
import sys
import logging
from PIL import Image, UnidentifiedImageError
import re
import uuid
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set level of logger

# Create a file handler
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
output_filename = f"import_{timestamp}.log"
file_handler = logging.FileHandler(output_filename)
file_handler.setLevel(logging.INFO)  # Set level of file handler

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Set level of console handler

# Create a formatter and add to handlers
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def get_basename(filename):
    "Extracts base part of filename without -size suffix"
    return filename.rsplit('-', 1)[0]


def get_images(s3, bucket_name, batch_size=5000):
    "Fetches and processes objects in an S3 bucket in batches, and saves the last processed key in a file."
    try:
        # Read the last processed key from a file if it exists
        try:
            with open("last_key.txt", "r") as file:
                last_key = file.read().strip()
        except FileNotFoundError:
            last_key = None

        # Start from the last processed key if it exists, otherwise fetch the first batch of objects
        if last_key:
            response = s3.list_objects(Bucket=bucket_name, Marker=last_key)
        else:
            response = s3.list_objects(Bucket=bucket_name)

        file_keys = []
        while response:
            file_keys.extend(response['Contents'])

            # process current batch if the size limit has been reached
            if len(file_keys) >= batch_size:
                # store the last processed key in a file
                with open("last_key.txt", "w") as file:
                    file.write(file_keys[-1]['Key'])

                logger.info(f"Batch of {len(file_keys)} images in the bucket {bucket_name}")
                return file_keys

            # Check if 'IsTruncated' is true in response, then the request is paginated
            if response['IsTruncated']:
                response = s3.list_objects(Bucket=bucket_name, Marker=file_keys[-1]['Key'])
            else:
                response = None

        # process remaining files in the last batch
        if file_keys:
            # store the last processed key in a file
            with open("last_key.txt", "w") as file:
                file.write(file_keys[-1]['Key'])

            logger.info(f"Final batch of {len(file_keys)} images in the bucket {bucket_name}")
            return file_keys
        else:  # No files found
            return None


    except NoCredentialsError:
        logger.info("Credentials not available")
        return None


def get_image_area(image):
    # Extract the size information from the filename
    size_info = re.sub(r'\.(jpg|jpeg|JPG|JPEG)$', '', image.rsplit('-', 1)[-1])
    try:
        width, height = map(int, size_info.split('x'))
        # Return the area
        return width * height
    except ValueError:
        # logger.info(f"Invalid size info '{size_info}' in image filename '{image}'. Skipping")
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
        logger.info(f"Error: {src_image} does not exist in the bucket {bucket_name}")
        return

    # Upscale the image
    try:
        with Image.open(file_name) as img:
            width, height = img.size
            aspect_ratio = width / height

            # only proceed if the image is >= 600 along the longest edge
            if max(width, height) >= 600 and aspect_ratio > 0.5 and aspect_ratio < 2:
                new_size = (width * upscale_factor, height * upscale_factor)
                upscaled_img = img.resize(new_size, Image.BICUBIC)  # Bicubic interpolation

                if object_exists(s3_client, bucket_name, base_image_name):  # If base image exists
                    temp_file_name = './tmp/' + str(uuid.uuid4()) + '.jpg'  # Create a temp file name
                    s3_client.download_file(bucket_name, base_image_name, temp_file_name)  # Download it
                    with Image.open(temp_file_name) as existing_img:  # Open it
                        ex_width, ex_height = existing_img.size  # Get its size
                        ex_aspect_ratio = ex_width / ex_height  # Get its aspect ratio
                        existing_img.close()

                        try:
                            os.remove(temp_file_name)  # Delete the temp file
                        except Exception as e:
                            logger.info(f"Error removing temp file {temp_file_name}: {e}")

                        if ((ex_width * ex_height) > (width * height)) or (((ex_width >= 2560) or (ex_height >=2560)) and ((ex_aspect_ratio > 0.5) or (ex_aspect_ratio <= 2))):  # If it's bigger or big enough
                            logger.info("Existing base image size is larger. Skipping the upscaling.")
                            return

                # If it didn't return until here, either base image doesn't exist, or upscaled image is not smaller
                dir_name = os.path.dirname(base_image_name)  # Get the directory name
                os.makedirs(dir_name, exist_ok=True)  # Create the directory if it doesn't exist
                upscaled_img.save(base_image_name)

                logger.info(f"Uploading upscaled image {base_image_name} to {bucket_name}/{base_image_name}")
                s3_client.upload_file(Filename=base_image_name, Bucket=bucket_name, Key=base_image_name)

                os.remove(base_image_name)  # Delete the upscaled image
            else:
                logger.info("Image size is less than 600 along its longest edge. Upscaling not performed.")

    except UnidentifiedImageError:
        logger.info(
            f"Unable to identify image {file_name}. It might not be an image or is unsupported/corrupt. Please check the file.")
        return

    finally:
        # Delete the downloaded image
        os.remove(file_name)


def main(bucket_name):
    # Create an S3 client
    s3_client = boto3.client('s3')

    images = get_images(s3_client, bucket_name)

    while images:
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
                        logger.info(f"Upscaling image {largest_file}")
                        try:
                            upscale_image(s3_client, bucket_name, largest_file, base_image_name)
                        except Exception as e:
                            logger.info(f"Error upscaling image {largest_file}: {e}")

            logger.info("-----------------")

        images = get_images(s3_client, bucket_name)

    logger.info("FINISHED!")
    os.remove("last_key.txt")


if __name__ == "__main__":
    main("jadore-models")
