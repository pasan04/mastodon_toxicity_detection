import os
import sys
import re
import gzip
import time
import logging
from urllib.parse import urlparse
from langdetect import detect
from googleapiclient import discovery
from googleapiclient.errors import HttpError
import json

# Processed data dir
processed_data_dir = "/media/processed_data"

# Processed mstdn files
processed_mstdn_files = "/media/mastodon_toxicity_detection/helper/processed_file_collection.txt"

GOOGLE_API_KEY = "AIzaSyBhlKZXCam9Wyhncupn-1fsgJO5TWS9S1A"

class MastodonProcessor:
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
        log_file = os.path.join(self.log_directory, "mstdn_analysis.log")

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

    def process_files(self, all_gz_files: list):
        """
        Process gzipped files, skipping already processed ones.

        Parameters:
        all_gz_files (list): List of gzipped files to process.
        processed_mstdn_files (str): File path to track processed files.
        """
        # Set up the logger
        logger = self.set_logger()

        # Read the processed files list (if it exists)
        processed_files = set()
        if os.path.exists(processed_mstdn_files):
            with open(processed_mstdn_files, 'r') as file:
                processed_files = set(line.strip() for line in file)

        for gz_file in all_gz_files:
            if gz_file in processed_files:
                logger.info(f"Skipping already processed file: {gz_file}")
                continue

            try:
                logger.info(f"Start processing file: {gz_file}")
                start_time = time.time()

                # Process the file line by line
                self.process_line(gz_file)

                end_time = time.time()
                file_processing_time = end_time - start_time
                logger.info(f"Finished processing file: {gz_file}, Time taken: {file_processing_time:.2f} seconds.")

                # After processing, mark the file as processed by appending it to the processed file
                with open(processed_mstdn_files, 'a') as file:
                    file.write(gz_file + '\n')

            except Exception as e:
                logger.error(f"Error in processing the file - {gz_file}: {e}")

    def process_line(self, gz_file: str) -> None:
        """
        Process lines in a gzipped file.

        Parameters:
        gz_file (str): Path to the gzipped file.
        """
        logger = self.set_logger()

        with gzip.open(gz_file, 'rt', encoding='utf-8') as file:
            for line in file:
                try:
                    json_obj = json.loads(line)

                    post_content = json_obj.get('content', '')
                    post_id = json_obj.get('id')
                    event_type = json_obj.get('event_type')

                    if event_type == 'update': # Check if the event type is update.
                        if not post_content.strip():
                            logger.warning(f"Skipping empty comment in post id: {post_id}")
                            continue

                        # Generate author file name and path
                        post_url = json_obj.get('account', {}).get('url')
                        if not post_url:
                            logger.warning(f"Skipping post id {post_id} due to missing account URL.")
                            continue

                        # Selected mstdn instances
                        selected_mstdn_instances = self.get_top_10_mstdn_instances()
                        domain, username = self.parse_domain_and_username(post_url, None)

                        if domain in selected_mstdn_instances and self.is_sentence_english(post_content):

                            # Analyze toxicity
                            toxicity_response = self.get_toxicity_score(post_content)
                            if not toxicity_response:
                                logger.warning(f"Skipping post id {post_id} due to API error.")
                                continue

                            error_type = toxicity_response.get("errorType")
                            if error_type == "LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE":
                                detected_languages = toxicity_response.get(
                                    "languageNotSupportedByAttributeError", {}
                                ).get("detectedLanguages", [])
                                logger.warning(f"Skipping unsupported language(s) {detected_languages} for post id: {post_id}")
                                continue

                            json_obj['toxicity_response'] = toxicity_response

                            author_file_name = self.parse_domain_and_username(post_url, "author_file")
                            file_path = os.path.join(processed_data_dir, f"{author_file_name}.json.gz")

                            # Convert the updated JSON object to a string
                            updated_line = json.dumps(json_obj)

                            # Open the gzipped file in append mode and write the updated line
                            with gzip.open(file_path, 'at') as out_f:  # Open file in text append mode ('at')
                                out_f.write(updated_line + '\n')

                except KeyError as ke:
                    logger.error(f"Missing key in JSON object: {ke}")
                except json.JSONDecodeError as je:
                    logger.error(f"Invalid JSON format: {je}")
                except Exception as e:
                    logger.error(f"Error processing line: {e}")

    def get_top_10_mstdn_instances(self) -> list:
        """
        Get the top 20 Mastodon instances by active users.
        url - https://instances.social/api/doc/

        Returns:
        list: A list of the top 20 Mastodon instances by active users
        """
        top_10_mstdn_instances = [
            "mastodon.social",
            "mstdn.social",
            "infosec.exchange",
            "mastodon.online",
            "mas.to",
            "techhub.social",
            "aethy.com",
            "hachyderm.io",
            "mastodon.world",
            "chaos.social"
        ]
        return top_10_mstdn_instances


    def parse_domain_and_username(self, user_identifier, data_type):
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
            if data_type == 'author_file':
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
        """
        Get toxicity score with retry on rate limit errors.
        """
        logger = self.set_logger()
        retries = 3  # Number of retries

        for attempt in range(retries):
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
                
                time.sleep(1.1)  # Delay between requests
                return response
            except HttpError as http_err:
                if "RATE_LIMIT_EXCEEDED" in str(http_err):
                    wait_time = (attempt + 1) * 10  # Exponential backoff
                    logger.warning(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"HTTP error during toxicity analysis: {http_err}")
                    break
            except Exception as e:
                logger.error(f"Error parsing post comment: {e}")
                break
        return None

    def is_sentence_english(self, sentence):
        try:
            language = detect(sentence)
            return language == 'en'
        except Exception as e:
            logger.error(f"Error in identifying the language: {e}")
            return False