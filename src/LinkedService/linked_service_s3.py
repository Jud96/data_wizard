
import boto3
from src.LinkedService.linked_service_to_data_lake import LinkedServiceToDataLake


class LinkedServiceS3(LinkedServiceToDataLake):
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.conn = self.connect()

    def connect(self):
        try:
            s3 = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                # region_name=self.region_name
            )
            return s3
        except Exception as e:
            print(f"Error connecting to S3: {e}")
            raise

    def get_metadata(self):
        try:
            s3 = self.conn
            response = s3.list_objects_v2(Bucket=self.bucket_name)
            metadata = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    metadata.append({
                        'Key': obj['Key'],
                        'LastModified': obj['LastModified'],
                        'Size': obj['Size'],
                        'StorageClass': obj['StorageClass']
                    })
            return metadata
        except Exception as e:
            print(f"Error fetching metadata from S3: {e}")
            raise
