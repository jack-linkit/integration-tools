import paramiko
import os
import zstandard as zstd
import tarfile
import tempfile
import shutil


def process_requestid_raw_files(sftp_client, request_id, target_dir, local_tmp_dir="tmp"):
    """
    For a given request_id, find the directory starting with request_id in /ETLProcessedFolder/001, 
    download all CSV files from that directory, and move them to target_dir.
    """
    remote_dir = "LinkIt/ETLProcessedFolder/001"
    
    try:
        files = sftp_client.listdir(remote_dir)
    except Exception as e:
        return False, 0, f"Error listing {remote_dir}: {str(e)}"

    # Find the directory that starts with request_id
    match_dir = None
    for f in files:
        if f.startswith(str(request_id)) and sftp_client.stat(f"{remote_dir}/{f}").st_mode & 0o40000:  # Check if it's a directory
            match_dir = f
            break
    
    if not match_dir:
        return False, 0, f"No directory found starting with {request_id} in {remote_dir} (searched both ETLProcessedFolder and BackupData locations)"
    else:
        print(f"Found directory: {match_dir}")

    remote_folder_path = f"{remote_dir}/{match_dir}"
    
    # List all files in the matched directory
    try:
        folder_files = sftp_client.listdir(remote_folder_path)
    except Exception as e:
        return False, 0, f"Error listing {remote_folder_path}: {str(e)}"

    # Filter for CSV and TXT files
    data_files = [f for f in folder_files if f.endswith('.csv') or f.endswith('.txt')]
    
    if not data_files:
        return False, 0, f"No CSV or TXT files found in {remote_folder_path}"

    # Ensure target_dir exists
    try:
        sftp_client.listdir(target_dir)
    except FileNotFoundError:
        try:
            sftp_client.mkdir(target_dir)
        except Exception as e:
            return False, 0, f"Failed to create target directory {target_dir}: {str(e)}"

    # Ensure local temp dir exists
    created_tmp_dir = False
    try:
        if not os.path.isdir(local_tmp_dir):
            os.makedirs(local_tmp_dir, exist_ok=True)
            created_tmp_dir = True
    except Exception as e:
        return False, 0, f"Failed to prepare local tmp dir '{local_tmp_dir}': {str(e)}"

    # Move data files to target_dir
    files_moved = 0
    errors = []
    
    for data_file in data_files:
        remote_source = f"{remote_folder_path}/{data_file}"
        remote_dest = f"{target_dir}/{data_file}"
        
        try:
            # Download to local temp first, then upload to target
            local_temp = os.path.join(local_tmp_dir, data_file)
            sftp_client.get(remote_source, local_temp)
            sftp_client.put(local_temp, remote_dest)
            
            # Clean up local temp file
            try:
                os.remove(local_temp)
            except Exception:
                pass
                
            files_moved += 1
            print(f"✓ Moved {data_file} for RequestID {request_id}")
        except Exception as e:
            errors.append(f"Failed to move {data_file}: {str(e)}")

    # Attempt to cleanup temp dir if we created it and it's empty
    if created_tmp_dir:
        try:
            if not os.listdir(local_tmp_dir):
                os.rmdir(local_tmp_dir)
        except Exception:
            pass

    if errors:
        return False, files_moved, f"Completed with errors: {'; '.join(errors)}"
    return True, files_moved, f"Successfully moved {files_moved} data files"


def process_requestid_file(sftp_client, request_id, target_dir, local_tmp_dir="tmp"):  # new function
    """
    For a given request_id, find the .tar.zst file in /BackupData/ETLProcessedFolder/001, download, decompress, extract CSV, and move to target_dir.
    """
    remote_dir = "LinkIt/BackupData/ETLProcessedFolder/001"
    # remote_dir = "LinkIt/ETLProcessedFolder/001"
    try:
        files = sftp_client.listdir(remote_dir)
    except Exception as e:
        return False, 0, f"Error listing {remote_dir}: {str(e)}"

    # Find the file
    match = None
    for f in files:
        if f.startswith(str(request_id)) and f.endswith(".tar.zst"):
            match = f
            break
    if not match:
        return False, 0, f"No .tar.zst file found for {request_id} in {remote_dir} (searched both ETLProcessedFolder and BackupData locations)"
    else: print(match)

    remote_path = f"{remote_dir}/{match}"

    # Ensure base temp dir exists and create a per-request working subdir
    try:
        os.makedirs(local_tmp_dir, exist_ok=True)
    except Exception as e:
        return False, 0, f"Failed to prepare base tmp dir '{local_tmp_dir}': {str(e)}"

    work_dir = os.path.join(local_tmp_dir, f"req_{request_id}")
    try:
        os.makedirs(work_dir, exist_ok=True)
    except Exception as e:
        return False, 0, f"Failed to prepare working tmp dir '{work_dir}': {str(e)}"

    local_zst = os.path.join(work_dir, match)
    local_tar = local_zst[:-4]  # remove .zst

    # Download
    try:
        sftp_client.get(remote_path, local_zst)
    except Exception as e:
        return False, 0, f"Failed to download {remote_path}: {str(e)}"

    # Decompress .zst to .tar
    try:
        with open(local_zst, "rb") as compressed, open(local_tar, "wb") as out:
            dctx = zstd.ZstdDecompressor()
            dctx.copy_stream(compressed, out)
    except Exception as e:
        return False, 0, f"Failed to decompress {local_zst}: {str(e)}"

    # Extract .csv from .tar
    csv_files = []
    try:
        with tarfile.open(local_tar, "r") as tar:
            for member in tar.getmembers():
                if member.name.endswith(".csv"):
                    tar.extract(member, path=work_dir)
                    csv_files.append(os.path.join(work_dir, member.name))
    except Exception as e:
        return False, 0, f"Failed to extract .tar: {str(e)}"

    if not csv_files:
        return False, 0, f"No .csv files found in {local_tar}"

    # Ensure target_dir exists
    try:
        sftp_client.listdir(target_dir)
    except FileNotFoundError:
        try:
            sftp_client.mkdir(target_dir)
        except Exception as e:
            return False, 0, f"Failed to create target directory {target_dir}: {str(e)}"

    # Move CSVs to target_dir (upload)
    files_moved = 0
    errors = []
    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        remote_dest = f"{target_dir}/{filename}"
        try:
            sftp_client.put(csv_path, remote_dest)
            files_moved += 1
            print(f"✓ Uploaded {filename} for RequestID {request_id}")
        except Exception as e:
            errors.append(f"Failed to upload {filename}: {str(e)}")

    # Cleanup local files
    for f in [local_zst, local_tar] + csv_files:
        try:
            os.remove(f)
        except Exception:
            pass

    # Attempt to remove any empty directories created by extraction (but don't touch base tmp unless we created it)
    extracted_dirs = []
    for csv_path in csv_files:
        dir_path = os.path.dirname(csv_path)
        if dir_path and dir_path != local_tmp_dir and dir_path not in extracted_dirs:
            extracted_dirs.append(dir_path)
    for d in sorted(extracted_dirs, key=lambda p: len(p.split(os.sep)), reverse=True):
        try:
            # Remove leaf directories upward until non-empty or reach base tmp
            rel = os.path.relpath(d, start=local_tmp_dir)
            if rel == ".":
                continue
            os.removedirs(d)
        except Exception:
            pass

    # Cleanup working directory entirely (safe, request-specific)
    try:
        shutil.rmtree(work_dir, ignore_errors=True)
    except Exception:
        pass

    if errors:
        return False, files_moved, f"Completed with errors: {'; '.join(errors)}"
    return True, files_moved, f"Successfully processed {files_moved} CSV files"


def process_requestids(requestid_to_target, hostname, username, password=None, key_path=None, local_tmp_dir="/tmp"):
    """
    Process multiple RequestIDs: download, decompress, extract, and move CSVs to target directories
    """
    transport = paramiko.Transport((hostname, 22))
    if key_path:
        private_key = paramiko.RSAKey.from_private_key_file(key_path)
        transport.connect(username=username, pkey=private_key)
    else:
        transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    results = {}
    try:
        for request_id, target_dir in requestid_to_target.items():
            print(f"\nProcessing RequestID: {request_id}")
            print(f"Target directory: {target_dir}")
            success, files_moved, message = process_requestid_file(
                sftp, request_id, target_dir, local_tmp_dir
            )
            results[request_id] = {
                "success": success,
                "files_moved": files_moved,
                "message": message,
            }
            print(f"Result: {message}")
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
    return results

# Example usage:
if __name__ == "__main__":
    requestid_to_target = {
        "12345": "/path/to/target/dir1",
        "67890": "/path/to/target/dir2",
        "11111": "/path/to/target/dir3",
    }
    hostname = "your.sftp.server"
    username = "your_username"
    password = "your_password"  # Or use key_path instead
    results = process_requestids(requestid_to_target, hostname, username, password)
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    for request_id, result in results.items():
        status = "✓ SUCCESS" if result["success"] else "✗ FAILED"
        print(f"RequestID {request_id}: {status} - {result['files_moved']} CSV files processed")
        if not result["success"]:
            print(f"  Error: {result['message']}")
