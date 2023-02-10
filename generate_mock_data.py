from utils.data_creation import create_sample_zip_files

if __name__ == '__main__':
    create_sample_zip_files(
        output_folder_path='data',
        num_files=2000,
        num_csv_files_per_zip=2
    )
