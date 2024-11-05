import glob
import os
from library.processor import MastodonProcessor

gz_file_dir = 'D:/applied_datascience/research/data'

# Get all the gz files.         
def list_gz_files(directory):
    gz_files = []
    for root, dirs, files in os.walk(directory):
        for file in glob.glob(os.path.join(root, "*.gz")):
            
            # Replace backslashes with forward slashes for cross-platform compatibility
            gz_files.append(file.replace("\\", "/"))
    return gz_files

if __name__ == '__main__':

    all_gzip_files = list_gz_files(gz_file_dir) # Get the gzip files. 

    data_processor = MastodonProcessor()
    data_processor.process_files(all_gzip_files)
