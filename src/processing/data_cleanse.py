import os
import shutil
import subprocess

def deep_clean():
    print("[CLEANER] Purging old test data to save space...")
    
    # 1. Clear Local Processed Data & Checkpoints
    paths_to_clear = ['data/processed/', 'data/checkpoints/']
    for path in paths_to_clear:
        if os.path.exists(path):
            shutil.rmtree(path)
            os.makedirs(path)
            print(f"[CLEANER] Reset local folder: {path}")

    # 2. Clear HDFS Temporary/Output Files
    #  keep the 'raw' folder in HDFS so I don't have to re-upload 2GB
    subprocess.run(["hadoop", "fs", "-rm", "-r", "/traceguard/processed/"], stderr=subprocess.DEVNULL)
    print("[CLEANER] Reset HDFS processed directory.")

    # 3. HBase Flush (Optional but helpful)
    # This forces HBase to move data from RAM (MemStore) to Disk (HFile)
    # Use this if HBase starts slowing down your system.
    # subprocess.run(["hbase", "shell"], input="flush 'threat_intel'", text=True)

if __name__ == "__main__":
    deep_clean()