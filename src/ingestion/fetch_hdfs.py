import os

def run_hdfs_ingestion():
    # Update this to the name of the file you extracted from HDFS_v1
    source_file = "data/raw/HDFS_large.log" 
    
    if os.path.exists(source_file):
        print(f"[INFO] Large HDFS logs (1.47GB) found at {source_file}")
        return source_file
    else:
        print(f"[ERROR] {source_file} missing. Please extract HDFS.log from HDFS_1.tar.gz")
        return None

if __name__ == "__main__":
    run_hdfs_ingestion()