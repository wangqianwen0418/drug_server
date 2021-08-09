try:
    import simplejson as json
except ImportError:
    import json
import numpy as np
import boto3
import botocore


def better_json_encoder(base_encoder):

    class JSONEncoder(base_encoder):
        """
        wrap json encoder to handle unserializable objects
        """

        def default(self, o):
            # convert np.float32 to float64, and round to  3 decimals
            if isinstance(o, np.float32):
                return np.round(np.float64(o), 3)
            return super(JSONEncoder, self).default(o)

    return JSONEncoder


def upload_file_to_s3(file, bucket_name, S3_KEY, S3_SECRET, filename):

    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET
    )

    try:

        s3.upload_fileobj(
            file,
            bucket_name,
            filename
        )

    except Exception as e:
        print("Something went wrong: ", e)
        return e

    return "upload {}".format('filename')
