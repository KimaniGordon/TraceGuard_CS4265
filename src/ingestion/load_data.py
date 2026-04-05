import subprocess
import os

def upload_to_hdfs():
    # 1. Dynamically find Hadoop and the local data
    hadoop_home = os.getenv("HADOOP_HOME", "C:/hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin", "hadoop.cmd")

    # Convert relative path to Absolute Path
    # It will turn 'data/raw/HDFS_large.log' into 'C:/Users/gordo/TraceGuard/data/raw/HDFS_large.log' or whatever the current working directory is
    relative_log_path = os.path.join("data", "raw", "HDFS_large.log")
    local_log_abs_path = os.path.abspath(relative_log_path)
    
    remote_path = "/traceguard/raw/logs/"

    print("--- TraceGuard Ingestion Initialized ---")
    print(f"[DEBUG] Working Directory: {os.getcwd()}")
    print(f"[DEBUG] Target Local File: {local_log_abs_path}")

    # 2. Check if the local file exists before attempting upload
    if not os.path.exists(local_log_abs_path):
        print(f"[ERROR] Local file NOT found at: {local_log_abs_path}")
        print("Tip: Make sure you are running the script from the 'TraceGuard' root folder.")
        return

    # 3. Create HDFS Directory
    print(f"[INFO] Ensuring HDFS directory {remote_path} exists...")
    subprocess.run([hadoop_bin, "fs", "-mkdir", "-p", remote_path], shell=True)

    # 4. Upload using the Absolute Path
    print(f"[INFO] Uploading data to HDFS. This will take a moment for 1.47 GB...")
    result = subprocess.run([hadoop_bin, "fs", "-put", "-f", local_log_abs_path, remote_path], shell=True)

    if result.returncode == 0:
        print("[SUCCESS] Payload delivered to HDFS.")
    else:
        print("[FAILURE] Hadoop 'put' command failed. Check if HDFS is in Safe Mode.")

if __name__ == "__main__":
    upload_to_hdfs()