import os
import gzip
import time
import logging

class MastodonProcessor:
    def __init__(self):
        """
        Initialize MastodonProcessor with data.
        """

        self.log_directory = "log"

        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

    def set_logger(self, gz_file:str) -> logging.Logger:
        """
        Setting up the logger for each file

        Passing parameters:
        gz_files (str): Processing gz file

        Returns:
        A logger configured to write to a specific file.
        """
        logger = logging.getLogger(f"Processor file - {gz_file}")
        logger.setLevel(logging.DEBUG)

        log_file = os.path.join(self.log_directory, f"processor_{gz_file}.log")

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG if self.debug_mode else logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        # logger handler 
        logger.addHandler(fh)

        # return the logger 
        return logger


    def process_files(self, all_gz_files):
        """
        Process a gzip file which is selected.
        """
        for gz_file in all_gz_files:
            self.author_file_save(gz_file)


    def author_file_save(self, gz_file: str):
        """
        Save author file. 
        """
        # this will setup the logger for the specific file. 
        logger = self.set_logger(gz_file)

        try:
            start_time = time.time()
            logger.info(f"Start processing file : {str(gz_file)}")

            with gzip.open(gz_file, 'rt') as f:

                for line in f:
                    line = line.rstrip()

                    if not line:
                        continue

            end_time = time.time()

            file_processing_time = end_time - start_time

            logger.info(f"End processing file: {str(gz_file)}, Time taken: {file_processing_time:.2f} seconds.")

        except Exception as e:
            logger.error(f"Error in processing the file - {gz_file} - {str(e)}")
