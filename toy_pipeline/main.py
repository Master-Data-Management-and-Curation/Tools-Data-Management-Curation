import subprocess
import sys

def run(script):
    subprocess.run([sys.executable, script], check=True)

def main():
    run("./storage/create_buckets.py")
    run("./storage/check_buckets.py")
    run("./nexus_preparation/prepare_file_upload.py")
    run("./storage/upload_file_buckets.py")

if __name__ == "__main__":
    main()
