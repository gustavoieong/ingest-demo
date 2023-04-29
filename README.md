# ingest-demo

# Install dependencies for REST API
sudo apt install python3-pip / sudo yum install python3-pip
sudo yum install git
pip3 install "fastapi[all]"

# Install dependencies for sending files
pip3 install requests

# Install dependencies for connecting database
pip3 install mysql-connector-python

# Clone repository
git clone https://github.com/gustavoieong/ingest-demo.git

# Execute main.py for testing
python3 -m uvicorn main:app --reload

# Send CSV files
python3 sender.py


