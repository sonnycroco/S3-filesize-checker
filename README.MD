## S3 File Checker - AWS SAM - Amazon Q

A mini AWS SAM project. When you upload a file to an S3 bucket:
- Files > 10 MB are automatically deleted.
- Files ≤ 10 MB are tagged with owner, TTL, upload time, and size.
- The Lambda logs an estimated monthly S3 cost for the object.


## About This Project

This project was developed with assistance from Amazon Q, AWS's AI-powered assistant. Amazon Q provided guidance on:
- AWS SAM template structure and best practices.
- Lambda function implementation with proper error handling.
- IAM policy configuration following least-privilege principles.
- S3 event integration and URL decoding handling.
- Step-by-step deployment and testing procedures.
- Cost estimation calculations and logging.


## Technologies Used

- AWS SAM, used this framework for the CloudFormation template
- AWS Lambda, serverless compute service to handle S3.
- Amazon S3, for storage.
- AWS Cloudformation, used automatically when $sam build.
- Amazon CloudWatch, for monitoring and logging AWS Lambda
- AWS IAM, for ID and access purposes.
- AWS CLI.
- Python3, for the Lambda function code.
- Boto3, for AWS interaction.
