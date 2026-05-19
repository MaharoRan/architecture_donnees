import os
import subprocess

SOURCE_DIR = 'source'
CONTAINER_NAME = 'namenode'
HDFS_BASE_DIR = '/data'

def run_cmd(cmd, check=True):
    print(f'Running: {" ".join(cmd)}')
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f'Error executing command: {result.stderr}')
    return result

def main():
    if not os.path.exists(SOURCE_DIR):
        print(f'Source directory "{SOURCE_DIR}" not found.')
        return

    files = [f for f in os.listdir(SOURCE_DIR) if os.path.isfile(os.path.join(SOURCE_DIR, f))]
    
    for file in files:
        file_path = os.path.join(SOURCE_DIR, file)
        file_name_no_ext, _ = os.path.splitext(file)
        
        # Target paths inside container
        container_file_path = f'/{file}'
        hdfs_dir = f'{HDFS_BASE_DIR}/{file_name_no_ext}'
        hdfs_file_path = f'{hdfs_dir}/{file}'

        print(f'\nProcessing {file}...')

        # 1. Check if the file is already in HDFS
        check_cmd = ['docker', 'exec', CONTAINER_NAME, 'hdfs', 'dfs', '-test', '-e', hdfs_file_path]
        check_res = run_cmd(check_cmd, check=False)
        
        if check_res.returncode == 0:
            print(f'File {file} already exists in HDFS at {hdfs_file_path}. Skipping.')
            continue

        # 2. Copy the file to the namenode container
        print(f'Copying {file} to namenode container...')
        cp_cmd = ['docker', 'cp', file_path, f'{CONTAINER_NAME}:{container_file_path}']
        run_cmd(cp_cmd)

        # 3. Create HDFS directory
        print(f'Creating HDFS directory {hdfs_dir}...')
        mkdir_cmd = ['docker', 'exec', CONTAINER_NAME, 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir]
        run_cmd(mkdir_cmd)

        # 4. Copy file from container to HDFS
        print(f'Putting {file} into HDFS...')
        put_cmd = ['docker', 'exec', CONTAINER_NAME, 'hdfs', 'dfs', '-put', container_file_path, hdfs_file_path]
        run_cmd(put_cmd)
        
        print(f'Successfully processed {file}.')

if __name__ == '__main__':
    main()
