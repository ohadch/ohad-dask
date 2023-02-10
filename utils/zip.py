import os
from zipfile import ZipFile

from dask.distributed import Client
from utils.logging import get_logger

logger = get_logger(__name__)


def unzip_zip_file(zip_file_path: str, output_folder_path: str):
    """
    Unzips a zip file.
    :param zip_file_path: The path to the zip file.
    :param output_folder_path: The path to the output folder.
    """
    logger.info(f"Unzipping zip file '{zip_file_path}' to '{output_folder_path}'")

    with ZipFile(zip_file_path, 'r') as zip_file:
        zip_file.extractall(output_folder_path)

    logger.info(f"Zip file '{zip_file_path}' unzipped to '{output_folder_path}'")


def unzip_all_zip_files_in_directory(client: Client, input_folder_path: str, output_folder_path: str):
    """
    Unzips all zip files in a directory.
    :param client: The dask client.
    :param input_folder_path: The path to the input folder.
    :param output_folder_path: The path to the output folder.
    """
    logger.info(f'Unzipping all zip files in {input_folder_path} to {output_folder_path}')

    futures = []
    for file_name in os.listdir(input_folder_path):
        if file_name.endswith('.zip'):
            future = client.submit(
                func=unzip_zip_file,
                zip_file_path=os.path.join(input_folder_path, file_name),
                output_folder_path=output_folder_path,
            )
            futures.append(future)

    client.gather(futures)

    logger.info(f'All zip files unzipped in {output_folder_path}')
