"""
This script is used to analyze the gzip files we collect and proceed.
"""
import os
import logging
import sys
import shutil
import gzip
import json
import re
import ijson
from urllib.parse import urlparse
from decimal import Decimal

processed_gz_dir = '/media/mastodon_toxicity_detection/processed_data'
separated_users_dir = '/media/mastodon_toxicity_detection/separated_users'
consolidated_dir = '/media/mastodon_toxicity_detection/user_info'
mean_toxicity_scores_dir = '/media/mastodon_toxicity_detection/mean_toxicity_scores'
all_user_details_dir = '/media/mastodon_toxicity_detection/user_details_all'

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
        Move the users to relevant instance folders
        """
        logger = self.set_logger()  # Setup the logger

        for root, dirs, files in os.walk(processed_gz_dir):
            for file in files:
                try:
                    if file.endswith(".gz"):
                        # Extract file name and user name
                        file_name, user_name = file.split('@')

                        # Create the path for the instance (based on file_name)
                        file_path = os.path.join(separated_users_dir, file_name)

                        # Create the directory if it doesn't exist
                        if not os.path.exists(file_path):
                            os.makedirs(file_path)
                            logger.info(f"Created directory: {file_path}")

                        # Construct the source and destination file paths
                        src_path = os.path.join(root, file)
                        dest_path = os.path.join(file_path, file)  # Ensure this points to the right location

                        # Copy the file to the destination
                        shutil.copy(src_path, dest_path)
                        logger.info(f"Copied file {file} to {dest_path}")

                except Exception as e:
                    logger.error(f"Error in processing the file {file}, error: {e}")

    def count_users_per_each_instance(self):
        """
        Count users per each instance 
        """
        logger = self.set_logger()  # Setup the logger

        instance_file_count = {}
        total_count = 0 # calculate the total

        for root, dirs, files in os.walk(separated_users_dir):
            for dir in dirs:
                dir_path = os.path.join(root, dir)

                gz_file_count = 0

                for file in os.listdir(dir_path):
                    # Check if the file is a .gz file
                    if file.endswith('.gz'):
                        gz_file_count += 1
                
                # Store the count of .gz files for this directory
                instance_file_count[dir] = gz_file_count
                total_count += gz_file_count
                # Log the count
                logger.info(f"Instance: {dir}, .gz files count: {gz_file_count}")
                logger.info(f"Total file count: {total_count}")


    def combine_all_users_to_one_file(self):
        """
        Combine all user data into consolidated files for each directory and calculate mean toxicity scores for each user.

        Args:
            separated_users_dir (str): Path to the directory containing separated user files.
            output_dir (str): Path to the directory to save consolidated files.
        """
        logger = self.set_logger()  # Setup the logger

        for root, dirs, _ in os.walk(separated_users_dir):
            for dir_name in dirs:
                dir_all_data = {}  # Dictionary to store data for the current directory
                dir_path = os.path.join(root, dir_name)

                logger.info(f"Processing directory: {dir_path}")

                for file_name in os.listdir(dir_path):
                    file_path = os.path.join(dir_path, file_name)
                    try:
                        with gzip.open(file_path, 'rt', encoding='utf-8') as file:  # Open as text
                            for line in file:
                                try:
                                    json_obj = json.loads(line.strip())

                                    # Extract account-specific fields
                                    acct_id = json_obj.get('account', {}).get('id', '')
                                    acct_url = json_obj.get('account', {}).get('url', '')
                                    acct_created_date = json_obj.get('account', {}).get('created_at', '')
                                    acct_uri = json_obj.get('account', {}).get('uri', '')
                                    domain, username = self.parse_domain_and_username(acct_url, None)
                                    domain_username = self.parse_domain_and_username(acct_url, 'author_file')

                                    # Extract account-specific fields
                                    acct_url = json_obj.get('account', {}).get('url', '')
                                    toxicity_score = json_obj.get('toxicity_response', {}).get(
                                        'attributeScores', {}).get('TOXICITY', {}).get('summaryScore', {}).get('value', 0)

                                    # Extract post-specific data
                                    post_data = {
                                        'post_id': json_obj.get('id', ''),
                                        'post_created_at': json_obj.get('created_at', ''),
                                        'post_uri': json_obj.get('uri', ''),
                                        'post_url': json_obj.get('url', ''),
                                        'content': json_obj.get('content', ''),
                                        'toxicity_score': toxicity_score,
                                        'post_language': json_obj.get('toxicity_response', {}).get('languages', ''),
                                    }

                                    # Check if account already exists in dir_all_data
                                    if acct_url in dir_all_data:
                                        dir_all_data[acct_url]['posts'].append(post_data)
                                        # Update total toxicity score and post count
                                        dir_all_data[acct_url]['total_toxicity_score'] += toxicity_score
                                        dir_all_data[acct_url]['total_posts'] += 1
                                    else:
                                        # Create a new entry for the account
                                        dir_all_data[acct_url] = {
                                            'acct_id': acct_id,
                                            'acct_url': acct_url,
                                            'acct_created_date': acct_created_date,
                                            'acct_uri': acct_uri,
                                            'domain': domain,
                                            'username': username,
                                            'acct_identifier': domain_username,
                                            'posts': [post_data],
                                            'total_toxicity_score': toxicity_score,
                                            'total_posts': 1,
                                            'mean_toxicity_score': 0  # Placeholder for now
                                        }
                                except json.JSONDecodeError as e:
                                    logger.error(f"JSON decode error in file {file_path}: {e}")

                    except Exception as e:
                        logger.error(f"Error reading file {file_path}: {e}")

                # Calculate mean toxicity score for each user
                for acct_url, data in dir_all_data.items():
                    if data['total_posts'] > 0:
                        data['mean_toxicity_score'] = data['total_toxicity_score'] / data['total_posts']

                # Define output file path for the consolidated data, using the directory name
                consolidated_file = os.path.join(consolidated_dir, f'{dir_name}_consolidated.json')

                try:
                    # Write the combined data for this directory to a JSON file
                    with open(consolidated_file, 'w', encoding='utf-8') as output_file:
                        json.dump(list(dir_all_data.values()), output_file, indent=4, ensure_ascii=False)
                    logger.info(f"Successfully saved data for directory {dir_path} to {consolidated_file}")
                except Exception as e:
                    logger.error(f"Error writing to output file {consolidated_file}: {e}")



                
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


    @staticmethod
    def custom_serializer(obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float for JSON serialization
        raise TypeError(f"Type {type(obj)} not serializable")

    def count_users_per_each_mstdn_instance(self):
        """
        Count the number of users per Mastodon instance with a mean_toxicity_score greater than 0.3,
        and save the results in a new JSON file named after the domain.

        Args:
            consolidated_dir (str): Path to the directory containing consolidated JSON files.
            user_details_dir (str): Path to the directory where output JSON files will be saved.
        """

        # Set up the logger
        logger = self.set_logger()

        # Get all JSON files in the directory
        all_json_files = os.listdir(consolidated_dir)

        # Process each file one by one
        for json_file in all_json_files:
            user_file = os.path.join(consolidated_dir, json_file)

            logger.info(f"Start processing file: {user_file}")

            instance_user_details = {}  # Reset for each file

            try:
                # Use ijson to parse the large JSON file incrementally
                with open(user_file, 'r', encoding='utf-8') as file:
                    # Use ijson to parse the JSON incrementally
                    parser = ijson.items(file, 'item')  # 'item' is the root element in the JSON file
                    for user in parser:
                        # Extract relevant fields
                        domain = user.get('domain', 'unknown')
                        acct_identifier = user.get('acct_identifier', None)
                        acct_url = user.get('acct_url', None)
                        mean_toxicity_score = user.get('mean_toxicity_score', 0)
                        total_toxicity_score = user.get('total_toxicity_score', 0)
                        total_posts = user.get('total_posts', 0)

                        # Check if mean toxicity score exceeds 0.3
                        # if mean_toxicity_score > 0.3:
                        if domain not in instance_user_details:
                            instance_user_details[domain] = []

                        # Append user details for the domain
                        instance_user_details[domain].append({
                            'acct_identifier': acct_identifier,
                            'acct_url': acct_url,
                            'total_toxicity_score': total_toxicity_score,
                            'total_posts': total_posts,
                            'mean_toxicity_score': mean_toxicity_score
                        })

            except Exception as e:
                logger.error(f"Error processing file {user_file}: {e}")
                continue

            # Write data for each domain immediately after processing the file
            for domain, users in instance_user_details.items():
                output_file = os.path.join(all_user_details_dir, f"{domain}.json")
                try:
                    # Write data to the file specific to the domain
                    with open(output_file, 'w', encoding='utf-8') as outfile:
                        json.dump(users, outfile, ensure_ascii=False, indent=4, default=ResearchAnalyser.custom_serializer)
                    logger.info(f"Results for domain {domain} successfully written to {output_file}")
                except Exception as e:
                    logger.error(f"Error writing to output file {output_file}: {e}")

    def calculate_users(self):
        """
        Calculate how many users in each Mstdn instances with toxicity score more than 0.3
        """
        # Set up the logger
        logger = self.set_logger()

        # Get all JSON files in the directory
        all_json_files = os.listdir(consolidated_dir)

        # Process each file one by one
        for json_file in all_json_files:
            user_file = os.path.join(consolidated_dir, json_file)

            logger.info(f"Start processing file: {user_file}")

            instance_user_details = {}  # Reset for each file
            user_count = 0
            try:
                # Use ijson to parse the large JSON file incrementally
                with open(user_file, 'r', encoding='utf-8') as file:
                    parser = ijson.items(file, 'item')
                    for user in parser:
                        user_count += 1
                logger.info(f"Total users with more than 0.3 toxicity in file - {json_file} and users - {user_count}")

            except Exception as e:
                logger.error(f"Error processing file {user_file}: {e}")
                continue

# Create an instance and call the method
if __name__ == "__main__":
    analyser = ResearchAnalyser()
    analyser.count_users_per_each_mstdn_instance()
    
