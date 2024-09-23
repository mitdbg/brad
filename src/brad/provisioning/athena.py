import boto3
from botocore.exceptions import ClientError


class AthenaProvisioning:
    # Initialize.
    def __init__(self, athena_id: str = "brad-db0"):
        self._athena_id = athena_id
        self._bucket_name = f"athena-{athena_id}"
        self._s3 = boto3.client("s3")
        self._athena = boto3.client("athena")
        self.create_s3_bucket()
        self.create_workgroup()

    # Try to create the bucket and catch the error if it already exists
    def create_s3_bucket(self):
        try:
            self._s3.create_bucket(
                Bucket=self._bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": self._s3.meta.region_name
                },
            )
            print(f"S3 Region: {self._s3.meta.region_name}")
            print("Bucket {} created successfully.".format(self._bucket_name))
        except ClientError as e:
            e_str = f"{e}"
            # pylint: disable-next=unsupported-membership-test
            if "BucketAlready" in e_str:
                print("Workgroup {} already exists.".format(self._athena_id))
                return
            else:
                print(f"RERAISING BRAD ERROR: {e}")
                raise e

    # Create workgroup.
    def create_workgroup(self):
        # Create the workgroup
        try:
            response = self._athena.create_work_group(
                Name=self._athena_id,
                Description="BRAD Workgroup.",
                Configuration={
                    "ResultConfiguration": {
                        "OutputLocation": f"s3://{self._bucket_name}/results/"
                    },
                    "PublishCloudWatchMetricsEnabled": True,
                },
            )
            print(f"Workgroup Results: {response}!")
        except ClientError as e:
            e_str = f"{e}"
            # pylint: disable-next=unsupported-membership-test
            if "WorkGroup is already" in e_str:
                print("Workgroup {} already exists.".format(self._athena_id))
                return
            else:
                print(f"RERAISING BRAD ERROR: {e}")
                raise e

    # Get Workgroup
    def get_workgroup(self) -> str:
        return self._athena_id
