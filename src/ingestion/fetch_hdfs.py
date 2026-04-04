import os
import shutil

def run_hdfs_ingestion():
    source_file = "data/raw/HDFS_2k.log"
    
    if os.path.exists(source_file):
        print(f"[INFO] HDFS logs found at {source_file}")
        # In a real pipeline, we might move/rename this for versioning
        return source_file
    else:
        print("[ERROR] hdfs_2k.log missing from data/raw/")
        return None

if __name__ == "__main__":
    run_hdfs_ingestion()