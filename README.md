# ingest-demo

# Install dependencies for REST API
apt install python3-pip
pip3 install "fastapi[all]"

# Install dependencies for sending files
pip3 install requests

# Install dependencies for connecting database
pip3 install mysql-connector-python

# Execute main.py for testing
python3 -m uvicorn main:app --reload

# Send CSV files
python3 sender.py

https://docs.amplify.aws/cli/start/install/#configure-the-amplify-cli

Data source is stored at ingestdemo S3 bucket.
