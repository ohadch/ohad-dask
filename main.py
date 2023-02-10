from dask.distributed import Client

from utils.logging import get_logger
from utils.zip import unzip_all_zip_files_in_directory

logger = get_logger(__name__)


def main():
    client = Client()
    unzip_all_zip_files_in_directory(
        client=client,
        input_folder_path='data',
        output_folder_path='output'
    )
    client.close()


if __name__ == '__main__':
    main()
