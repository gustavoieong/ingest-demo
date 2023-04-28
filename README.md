# ingest-demo

#Install dependencies
sudo apt install python3-pip
pip install "fastapi[all]"

#Execute main.py for testing
python3 -m uvicorn main:app --reload



Data source is stored at ingestdemo S3 bucket.
