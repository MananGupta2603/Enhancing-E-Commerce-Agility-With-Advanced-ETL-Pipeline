# Import the required modules
import streamlit as st
import boto3
from io import BytesIO
from datetime import datetime
import time
import credentials

# AWS credentials and configuration
AWS_REGION = credentials.AWS_REGION
AWS_ACCESS_KEY = credentials.AWS_ACCESS_KEY
AWS_SECRET_KEY = credentials.AWS_SECRET_KEY

# S3 bucket names
ORDER_BUCKET = credentials.ORDER_BUCKET
ORDER_FOLDER = credentials.ORDER_FOLDER
RETURN_BUCKET = credentials.RETURN_BUCKET
RETURN_FOLDER = credentials.RETURN_FOLDER

# Step Function ARN
STEP_FUNCTION_ARN = credentials.STEP_FUNCTION_ARN

# Initialize AWS clients
s3_client = boto3.client(
    service_name="s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

sf_client = boto3.client(
    service_name="stepfunctions",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

# Function to upload file to S3
def s3_upload_file(file, bucket, folder_name, s3_file_name):
    try:
        s3_key = f"{folder_name}/{s3_file_name}"  # Full S3 path including folder
        s3_client.upload_fileobj(file, bucket, s3_key)
        return f"File successfully uploaded to S3: s3://{bucket}/{s3_key}"
    except Exception as e:
        return f"Error uploading file to S3: {e}"

# Function to process and upload a file
def file_uploader(file, bucket, folder_name):
    file_data = BytesIO(file.getvalue())
    original_file_name = file.name
    tstamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{original_file_name}"
    return s3_upload_file(file_data, bucket, folder_name, file_name)

# Main Streamlit application
st.title("E-Commerce Data Upload Portal")

with st.form("File Uploader", clear_on_submit=True):
    st.subheader("Upload Files")
    orders_file = st.file_uploader("Upload your 'Orders' CSV file:", type="csv", key="Orders")
    returns_file = st.file_uploader("Upload your 'Returns' CSV file:", type="csv", key="Returns")
    submitted = st.form_submit_button("Upload Files")

    # Placeholders for messages
    message_placeholder = st.empty()
    progress_placeholder = st.empty()

    if submitted:
        if orders_file is not None and returns_file is not None:
            # Upload files to their respective buckets and folders
            upload_time = datetime.now()
            message_placeholder.info("Uploading files...")
            time.sleep(10)

            # Clear the previous message and show a new one
            upload_message = file_uploader(orders_file, ORDER_BUCKET, ORDER_FOLDER)
            message_placeholder.empty()  # Clear the previous message
            message_placeholder.success(upload_message)
            time.sleep(5)
            # Upload the returns file and display a message
            upload_message = file_uploader(returns_file, RETURN_BUCKET, RETURN_FOLDER)
            message_placeholder.success(upload_message)

            time.sleep(10)

            # Trigger Step Function and monitor the workflow
            message_placeholder.empty()
            message_placeholder.info("Triggering Step Function workflow...")

            time.sleep(10)
            try:
                response = sf_client.start_execution(stateMachineArn=STEP_FUNCTION_ARN)
                execution_arn = response["executionArn"]
                message_placeholder.empty()  # Clear the previous message
                message_placeholder.success(f"Step Function workflow started: {execution_arn}")

                # Monitor workflow status with dynamic progress
                message_placeholder.empty()
                result = st.warning("ETL job is in progress...")
                complete = False
                while not complete:
                    time.sleep(5)  # Poll every 5 seconds
    
                    # Check current status of Step Function
                    status_response = sf_client.describe_execution(executionArn=execution_arn)
                    status = status_response["status"]
                    if status == "SUCCEEDED":
                        result.empty()
                        message_placeholder.success("ETL job succeeded! Data has been processed.")
                        complete = True
                    elif status == "FAILED":
                        result.empty()
                        message_placeholder.error("ETL job failed. Check Step Function logs for details.")
                        complete = True

                # Clear progress bar once the job is complete
                progress_placeholder.empty()
            except Exception as e:
                message_placeholder.empty()
                message_placeholder.error(f"Error triggering Step Function: {e}")
        else:
            message_placeholder.warning("Please upload both the Orders and Returns files.")
