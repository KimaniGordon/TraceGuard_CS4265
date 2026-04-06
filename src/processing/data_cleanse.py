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
    print("[CLEANER] Attempting to reset HDFS processed directory...")
    try:
        # Adding shell=True and check=False so it doesn't crash if the folder doesn't exist yet
        subprocess.run("hadoop fs -rm -r /traceguard/processed/", shell=True, stderr=subprocess.DEVNULL, check=False)
        print("[CLEANER] Reset HDFS processed directory.")
    except Exception as e:
        print(f"[CLEANER] Skip HDFS cleanup: {e} (Hadoop might not be in PATH)")

    # 3. HBase Flush (Optional but helpful)
    # This forces HBase to move data from RAM (MemStore) to Disk (HFile)
    # Use this if HBase starts slowing down your system.
    # subprocess.run(["hbase", "shell"], input="flush 'threat_intel'", text=True)

if __name__ == "__main__":
    deep_clean()