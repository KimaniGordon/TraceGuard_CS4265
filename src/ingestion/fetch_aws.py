import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os

def download_aws_subset():
    bucket_name = 'cse-cic-ids2018'
    # I am picking Friday because it has the 'Botnet' data important for representative testing, and the file size is manageable for my 10.2GB limit
    file_key = 'Processed Traffic Data for ML Algorithms/Friday-02-03-2018_TrafficForML_CICFlowMeter.csv'
    local_path = 'data/raw/network_traffic/aws_subset.csv'
    
    os.makedirs('data/raw/network_traffic', exist_ok=True)
    
    print(f"[INFO] Fetching {file_key} from AWS Public Registry...")
    
    # Configure S3 to allow anonymous (unsigned) access
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    try:
        s3.download_file(bucket_name, file_key, local_path)
        print(f"[SUCCESS] Representative subset saved to {local_path}")
        # Verify the file size fits the 10.2GB limit
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"[INFO] Downloaded size: {size_mb:.2f} MB")
    except Exception as e:
        print(f"[ERROR] AWS Download failed: {e}")

if __name__ == "__main__":
    download_aws_subset()