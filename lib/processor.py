import os
import re
import gzip
import time
import logging
from urllib.parse import urlparse
from googleapiclient import discovery
import json

# Processed data dir
processed_data_dir = "/Users/pkamburu/mastodon_toxicity_detection/processed_data"
GOOGLE_API_KEY = "AIzaSyBhlKZXCam9Wyhncupn-1fsgJO5TWS9S1A"

class MastodonProcessor:
    def __init__(self):
        """
        Initialize Mastodon processor with data.
        """
        self.debug_mode = True
        self.log_directory = "log"

        # Ensure the log directory exists
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

        # Correct log file path
        log_file = os.path.join(self.log_directory, "mstdn_analysis.log")

        # Create a file handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG if self.debug_mode else logging.INFO)

        # Set formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        # Avoid adding duplicate handlers
        if not logger.handlers:
            logger.addHandler(fh)

        return logger

    def process_files(self, all_gz_files: list):
        """
        Process gzipped files.

        Parameters:
        all_gz_files (list): List of gzipped files to process.
        """
        # Set up the logger
        logger = self.set_logger()

        for gz_file in all_gz_files:
            try:
                logger.info(f"Start processing file: {gz_file}")
                start_time = time.time()

                # Process the file line by line
                self.process_line(gz_file)

                end_time = time.time()
                file_processing_time = end_time - start_time
                logger.info(f"Finished processing file: {gz_file}, Time taken: {file_processing_time:.2f} seconds.")
            except Exception as e:
                logger.error(f"Error in processing the file - {gz_file}: {e}")

    def process_line(self, gz_file: str) -> None:
        """
        Process lines in a gzipped file.

        Parameters:
        gz_file (str): Path to the gzipped file.
        """
        logger = self.set_logger()

        with gzip.open(gz_file, 'rt') as f:
            for line in f:
                line = line.rstrip()
                if not line:
                    continue
                try:
                    # Convert the line into json
                    json_obj = json.loads(line)
                    post_url = json_obj['account']['url']

                    selected_mstdn_instances = self.get_top_20_mstdn_instances()

                    if post_url:
                        domain, username = self.parse_domain_and_username(post_url, None)

                        if domain in selected_mstdn_instances:
                            author_file_name = self.parse_domain_and_username(post_url, "author_file")
                            file_path = os.path.join(processed_data_dir, f"{author_file_name}.json.gz")
                            post_content = json_obj['content']

                            toxicity_response = self.get_toxicity_score(post_content)
                            json_obj['toxicity_response'] = toxicity_response  # Add the entire response to the JSON object

                            # Convert the updated JSON object to a string
                            line = json.dumps(json_obj)

                            # Open the gzipped file in append mode and write the updated line
                            with gzip.open(file_path, 'at') as out_f:  # Open file in text append mode ('at')
                                out_f.write(line + '\n')

                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}")
                    continue

    def get_top_20_mstdn_instances(self) -> list:
        """
        Get the top 20 Mastodon instances by active users.
        url - https://instances.social/api/doc/

        Returns:
        list: A list of the top 20 Mastodon instances by active users
        """
        top_20_mstdn_instances = [
                "mastodon.social",
                "pawoo.net",
                "mstdn.jp",
                "mstdn.social",
                "infosec.exchange",
                "mastodon.online",
                "mas.to",
                "fosstodon.org",
                "fedibird.com",
                "hachyderm.io",
                "mastodon.world",
                "m.cmx.im",
                "chaos.social",
                "troet.cafe",
                "mastodon.gamedev.place",
                "piaille.fr",
                "aethy.com",
                "planet.moe",
                "techhub.social"
        ]
        return top_20_mstdn_instances


    def parse_domain_and_username(self, user_identifier, type):
        """
        Extract the domain and username from a URL
        and return it as a string in the format `username@domain`
        if the type is 'author_file', otherwise return a tuple (domain, username).

        Parameters:
        user_identifier (str): The URL of the user.
        type (str): The type of return value ('author_file' for string, otherwise tuple).

        Returns:
        str or tuple: 'username@domain' if type is 'author_file', else (domain, username).
        """
        # Set up the logger
        logger = self.set_logger()

        try:
            parsed_url = urlparse(user_identifier)
            if type == 'author_file':
                clean_user_identifier = re.sub(r'/$', '', user_identifier)
                parsed_url = urlparse(clean_user_identifier)
                domain = parsed_url.hostname
                username = parsed_url.path.split('/')[-1].strip("@")
                return f"{domain}@{username}"
            else:
                domain = parsed_url.netloc
                # Split the path and filter out empty parts
                path_parts = [part for part in parsed_url.path.split('/') if part]
                # Get the last non-empty part of the path as the username
                username = path_parts[-1].strip("@") if path_parts else ""
            return domain, username
        except Exception as e:
            logger.error(f"Error parsing URL: {e}")
            return None

    def get_toxicity_score(self, post_content):
        # Set up the logger
        logger = self.set_logger()

        # Refer here - https://developers.perspectiveapi.com/s/docs-sample-requests?language=en_US
        try:
            client = discovery.build(
                "commentanalyzer",
                "v1alpha1",
                developerKey=GOOGLE_API_KEY,
                discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
                static_discovery=False,
            )

            analyze_request = {
                'comment': {'text': post_content},
                'requestedAttributes': {'TOXICITY': {}}
            }

            response = client.comments().analyze(body=analyze_request).execute()
            return response
        except Exception as e:
            logger.error(f"Error parsing URL: {e}")
            return None