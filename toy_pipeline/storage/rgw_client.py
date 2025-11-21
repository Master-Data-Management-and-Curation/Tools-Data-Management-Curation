# rgw_client.py

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os
import requests
import json

class RGWClient:
    

    def __init__(self, endpoint, access_key, secret_key, region="default", verify=False):
        self.endpoint = endpoint
        self.auth = (access_key, secret_key)
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name=region,
            verify=verify
        )
    def put_bucket_policy(self, bucket_name, policy):
        resp = self.s3.put_bucket_policy(
        Bucket=bucket_name,
        Policy=policy
        )

    def list_buckets(self):
        """list the buckets"""
        try:
            resp = self.s3.list_buckets()
            return [b["Name"] for b in resp.get("Buckets", [])]
        except ClientError as e:
            raise RuntimeError(f"Error: {e}")

    def get_bucket_tags(self, bucket_name):
        """dict for the tag(Key→Value)."""
        try:
            resp = self.s3.get_bucket_tagging(Bucket=bucket_name)
            return {t["Key"]: t["Value"] for t in resp["TagSet"]}
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("NoSuchTagSet", "NoSuchBucket"):
                return {}
            raise RuntimeError(f"Error '{bucket_name}': {e}")

    def create_bucket(self, bucket_name):
        """new bucket."""
        try:
            self.s3.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            raise RuntimeError(f"Erro'{bucket_name}': {e}")

    def put_bucket_tags(self, bucket_name, tags_dict):
        """tag apply."""
        tagset = [{"Key": k, "Value": v} for k, v in tags_dict.items()]
        try:
            self.s3.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": tagset})
        except ClientError as e:
            raise RuntimeError(f"Error'{bucket_name}': {e}")
    def add_bucket_tags(self, bucket_name: str, new_tags: dict):
        
        try:
            # Recupera i tag esistenti
            try:
                current = self.s3.get_bucket_tagging(Bucket=bucket_name)
                tag_set = {t["Key"]: t["Value"] for t in current["TagSet"]}
            except self.s3.exceptions.ClientError as e:
                # if tag do not exist create a dict
                if e.response["Error"]["Code"] in ["NoSuchTagSet", "NoSuchTagSetError"]:
                    tag_set = {}
                else:
                    raise

            # tag updat
            tag_set.update(new_tags)

            # conversion
            tag_list = [{"Key": k, "Value": v} for k, v in tag_set.items()]

            # Apply
            self.s3.put_bucket_tagging(
                Bucket=bucket_name,
                Tagging={"TagSet": tag_list}
            )
            return True

        except Exception as e:
            raise RuntimeError(f"Error '{bucket_name}': {e}")

    def upload_file(self, bucket_name, local_path, object_name=None):
        
        try:
            if object_name is None:
                object_name = os.path.basename(local_path)

            self.s3.upload_file(local_path, bucket_name, object_name)
            return f"File '{object_name}' upload in bucket '{bucket_name}'"
        except ClientError as e:
            raise Exception(f"Error")
   
    def delete_bucket(self, bucket_name):
        try:
        # delete objects inside and then bucket
            objects = self.s3.list_objects_v2(Bucket=bucket_name)
            if "Contents" in objects:
              for obj in objects["Contents"]:
                self.s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        
            self.s3.delete_bucket(Bucket=bucket_name)
            return True
        except Exception as e:
           raise RuntimeError(f"Error {bucket_name}: {e}")

    def list_objects(self, bucket_name):
    
      try:
        response = self.s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            return []  # Bucket empty
        objects = [obj['Key'] for obj in response['Contents']]
        return objects
      except Exception as e:
        raise Exception(f"Error '{bucket_name}': {e}")    
