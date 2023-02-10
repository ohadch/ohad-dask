import os
import random
import zipfile
from tempfile import TemporaryDirectory

import pandas as pd

from utils.logging import get_logger

logger = get_logger(__name__)


LIMIT_CREATE_SAMPLE_CSV_FILES = 2000  # The maximum number of files to create (to avoid creating too many files).
LIMIT_CREATE_SAMPLE_ZIP_FILES = 2000  # The maximum number of files to create (to avoid creating too many files).


def create_sample_csv_file(output_file_path: str):
    """
    Creates a sample csv file with two columns and three rows.
    :param output_file_path: The path to the output file.
    """
    df = pd.DataFrame(data={'col1': random.sample(range(100), 3), 'col2': random.sample(range(100), 3)})
    df.to_csv(output_file_path, index=False)


def create_sample_csv_files(output_folder_path: str, num_files: int, start_index: int = 0):
    """
    Creates two sample csv files with two columns and three rows.
    :param output_folder_path: The path to the output folder.
    :param num_files: The number of files to create.
    :param start_index: The index to start at when naming the files.
    """
    if num_files > LIMIT_CREATE_SAMPLE_CSV_FILES:
        raise ValueError(f'num_files ({num_files}) must be less than or equal to {LIMIT_CREATE_SAMPLE_CSV_FILES}')

    logger.info(f'Creating {num_files} sample csv files in {output_folder_path}')

    for i in range(num_files):
        file_path = output_folder_path + f'/file_{i + start_index}.csv'
        logger.info(f'Creating file {i}/{num_files}: {file_path}')
        create_sample_csv_file(output_file_path=file_path)


def create_zip_file_with_n_csv_files(output_file_path: str, num_files: int, start_index: int = 0):
    """
    Creates a zip file with n csv files.
    :param output_file_path: The path to the output file.
    :param num_files: The number of csv files to add to the zip file.
    :param start_index: The index to start at when naming the csv files.
    """
    logger.info(f'Creating zip file with {num_files} csv files: {output_file_path}')

    with TemporaryDirectory() as temp_dir:
        create_sample_csv_files(output_folder_path=temp_dir, num_files=num_files, start_index=start_index)
        with zipfile.ZipFile(output_file_path, 'w') as zip_file:
            for file_name in os.listdir(temp_dir):
                zip_file.write(temp_dir + '/' + file_name, arcname=file_name)

    logger.info(f'Zip file created: {output_file_path}')


def create_sample_zip_files(
        output_folder_path: str,
        num_files: int,
        num_csv_files_per_zip: int
):
    """
    Creates two sample zip files with two csv files each.
    :param output_folder_path: The path to the output folder.
    :param num_files: The number of files to create.
    :param num_csv_files_per_zip: The number of csv files to add to each zip file.
    """
    if num_files > LIMIT_CREATE_SAMPLE_ZIP_FILES:
        raise ValueError(f'num_files ({num_files}) must be less than or equal to {LIMIT_CREATE_SAMPLE_ZIP_FILES}')

    logger.info(f'Creating {num_files} sample zip files in {output_folder_path}')

    for i in range(num_files):
        file_path = output_folder_path + f'/file_{i}.zip'
        logger.info(f'Creating file {i}/{num_files}: {file_path}')
        create_zip_file_with_n_csv_files(output_file_path=file_path, num_files=num_csv_files_per_zip, start_index=i * num_csv_files_per_zip)

    logger.info(f'Zip files created: {num_files}')