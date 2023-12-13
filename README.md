# wordpress-image-fixer
If original image is missing, re-generates it from the largest resized image.  For offloaded s3 images.

## Install AWS CLI
https://aws.amazon.com/cli/

### Configure CLI with credentials
https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html
```
aws configure
```

## Change chosen bucket name in main.py

Then run it!

Can probably be relatively easily adapted to work with the normal wordpress uploads directory.
