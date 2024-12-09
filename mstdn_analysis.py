import os
from lib.processor import MastodonProcessor

gz_file_dir = '/media/mstdn_data_batch_1'

# Get selected the gz files.
def list_gz_files(directory):
    """
    Grab all gzip files from the directory that match selected Mastodon instances.

    Parameters:
    directory (str): Path to the directory to search.

    Returns:
    list: List of matching gzip file paths.
    """
    # Define the selected Mastodon instances
    selected_instances = [
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

    gz_files = []

    # Walk through the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".gz"):
                # Replace backslashes with forward slashes for compatibility
                normalized_file = os.path.join(root, file).replace("\\", "/")

                # Extract the base name of the file
                base_name = os.path.basename(normalized_file)

                # Check if the base name starts with any of the selected instances
                if any(base_name.startswith(instance + "_") for instance in selected_instances):
                    gz_files.append(normalized_file)

    return gz_files


if __name__ == '__main__':
    all_gzip_files = list_gz_files(gz_file_dir)
    data_processor = MastodonProcessor()
    # Send all the collected gzip files to data processor
    data_processor.process_files(all_gzip_files)
