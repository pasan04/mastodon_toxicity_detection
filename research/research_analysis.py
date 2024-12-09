"""
This script is used to analyze the gzip files we collect and proceed.
"""
import os
import logging
import sys
import shutil

processed_gz_dir = '/media/processed_data'
separated_users_dir = '/media/separated_users'


class ResearchAnalyser:
    def __init__(self):
        """
        Initialize Mastodon processor with data.
        """
        self.debug_mode = True
        self.log_directory = "helper"

        # Ensure the helper directory exists
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

    def set_logger(self) -> logging.Logger:
        """
        Setting up the logger.

        Returns:
        A logger configured to write to a specific file.
        """
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        # Correct helper file path
        log_file = os.path.join(self.log_directory, "research_info.log")

        # Check if handlers already exist to avoid duplicates
        if not logger.handlers:
            # Create a file handler
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.DEBUG if self.debug_mode else logging.INFO)

            # Set formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            logger.addHandler(fh)

            # Add a stream handler for stdout
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(formatter)
            ch.setLevel(logging.INFO)
            logger.addHandler(ch)

        return logger

    def count_authors(self):
        """
        Count how many users we processed.
        """
        logger = self.set_logger()
        author_count = 0  # Initialize the author count

        # Traverse through the directory
        for root, dirs, files in os.walk(processed_gz_dir):
            for file in files:
                if file.endswith(".gz"):
                    author_count += 1

        logger.info(f"Total number of users collected: {author_count}")

    def separate_users_to_instances(self):
        """
        Move the users to relavant instances folders
        """
        logger = self.set_logger() # Setup the logger

        for root, dirs, files in os.walk(processed_gz_dir):
            for file in files:
                try: 
                    if file.endswith(".gz"):
                        file_name, user_name = file.split('@')

                        file_path = os.path.join(separated_users_dir, file_name)
                        if os.path.exists(file_path):
                            os.makedirs(file_path)
                            logger.error(f"Created directory : {file_path}")
                        
                        # Construct the full file path
                        src_path = os.path.join(root, file)
                        dest_path = os.path.join(separated_users_dir, file)

                        # Construct the full file path
                        src_path = os.path.join(root, file)
                        dest_path = os.path.join(file_path, file)

                        # Copy the file
                        shutil.copy(src_path, dest_path)
                        logger.info(f"Copied file {file} to {dest_path}")

                except Exception as e:
                    logger.error(f"Error in processing the file {file}, error : {e}" )

# Create an instance and call the method
if __name__ == "__main__":
    analyser = ResearchAnalyser()
    analyser.separate_users_to_instances()
