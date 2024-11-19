import glob
import os
from lib.processor import MastodonProcessor

gz_file_dir = ''

# Get all the gz files.         
def list_gz_files(directory):
    """
    Grab all gzip files from the directory.
    """
    gz_files = []
    for root, dirs, files in os.walk(directory):
        for file in glob.glob(os.path.join(root, "*.gz")):
            # Replace backslashes with forward slashes for cross-platform compatibility
            gz_files.append(file.replace("\\", "/"))
    return gz_files

if __name__ == '__main__':
    all_gzip_files = list_gz_files(gz_file_dir)
    data_processor = MastodonProcessor()
    # Send all the collected gzip files to data processor
    data_processor.process_files(all_gzip_files)
