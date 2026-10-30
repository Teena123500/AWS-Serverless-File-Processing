import boto3
import json
import urllib.parse
import os
from datetime import datetime, timezone

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

def lambda_handler(event, context):

    for record in event["Records"]:

        bucket = record["s3"]["bucket"]["name"]

        key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )
        if key.startswith("processed-logs/"):
             continue

        # Read uploaded file
        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        content = response["Body"].read()

        # Create processed metadata
        metadata = {
            "original_bucket": bucket,
            "file_name": key,
            "file_size_bytes": len(content),
            "status": "Processed Successfully",
            "processed_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        # Save metadata log in S3
        log_key = "processed-logs/" + key + ".json"

        s3.put_object(
            Bucket=bucket,
            Key=log_key,
            Body=json.dumps(metadata, indent=4),
            ContentType="application/json"
        )

        # Send SNS notification
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="File Processing Successful",
            Message=json.dumps(metadata, indent=4)
        )

    return {
        "statusCode": 200,
        "body": "File processed successfully"
    }
