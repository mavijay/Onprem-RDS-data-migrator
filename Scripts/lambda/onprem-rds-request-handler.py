import json
import traceback
import os
import boto3
from botocore.exceptions import ClientError

glue_con = boto3.client(service_name='glue')
count = 0


def lambda_handler(event, context):
    """
    Handler
    """
    try:
        response = {}
        glue_response = {}
        for record in event["Records"]:
            bucket_name = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            response["bucket_name"] = bucket_name
            response["key"] = key
            response["host"] = os.environ["HOST"]
            response["user"] = os.environ["USER"]
            response["password"] = os.environ["PASSWORD"]
            response["dbName"] = os.environ["dbName"]
            response["port"] = os.environ["port"]
        print(f'response of the s3 bucket : {response}')
        bucket_name = response.get("bucket_name")
        key = response.get("key")
        config = json.dumps(response)
        if "tables" in key:
            result = objectCount(bucket_name, key)
            count = len(result["Contents"])
            print(f'count of all the files present in s3 : {count}')
            if response.get("key").split("/")[-1].split(".")[0].lower() == "dummy" and count == 4:
                glue_response = invoke_glue_job(config)
    except Exception as e:
        print("failed into lambda_handler", e)
    return "lambda exe failed"


def invoke_glue_job(config):
    try:
        glue_job_name = os.environ['GLUE_JOB_NAME']
        executed_glue_job = glue_con.start_job_run(JobName=glue_job_name,
                                                   Arguments={
                                                       '--config_json': config
                                                   }
                                                   )

    except Exception as e:
        message = 'Unable to invoke Glue Job: ' + glue_job_name
        print(message, e)
        raise ClientError(traceback.format_exc())
    return executed_glue_job


def objectCount(bucket_name, key):
    try:
        s3_client = boto3.client("s3")
        s_count = key.count('/')
        prefix = "/".join(key.split("/")[0:s_count])
        print(f"prefix:{prefix}")
        print(f"bucket_name:{bucket_name}")
        result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        print(f"result of file list:{result}")
    except Exception as e:
        print(e)
        raise
    return result
