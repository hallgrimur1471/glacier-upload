# A simple python script to upload files to AWS Glacier vaults.
# Copyright (C) 2016 Trapsilo P. Bumi tbumi@thpd.io
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import binascii
import concurrent.futures
import hashlib
import math
import os
import os.path
import sys
import tarfile
import tempfile
import threading
import pathlib
import datetime
import typing
from typing import List

import boto3
import click

MAX_ATTEMPTS = 10

fileblock = threading.Lock()


def timed_print(msg):
    now = datetime.datetime.now()
    hms_string = f"{now.strftime('%H:%M:%S')}.{now.strftime('%f')[0:3]}"
    click.echo(f"[{hms_string}] {msg}")


def is_regular_file(file_path: pathlib.Path):
    if file_path.is_file() and not file_path.is_symlink():
        return True
    return False


def human_readable_bytes(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def calculate_total_tree_hash(list_of_checksums):
    tree = list_of_checksums[:]
    while len(tree) > 1:
        parent = []
        for i in range(0, len(tree), 2):
            if i < len(tree) - 1:
                part1 = binascii.unhexlify(tree[i])
                part2 = binascii.unhexlify(tree[i + 1])
                parent.append(hashlib.sha256(part1 + part2).hexdigest())
            else:
                parent.append(tree[i])
        tree = parent
    return tree[0]


def calculate_tree_hash(part, part_size):
    checksums = []
    upper_bound = min(len(part), part_size)
    step = 1024 * 1024  # 1 MB
    for chunk_pos in range(0, upper_bound, step):
        chunk = part[chunk_pos : chunk_pos + step]
        checksums.append(hashlib.sha256(chunk).hexdigest())
        del chunk
    return calculate_total_tree_hash(checksums)


def calculate_directory_size(directory: pathlib.Path):
    """
    Returns total size in bytes of all regular files recursively under
    directory. Ignores symlinks.
    """
    if not directory.is_dir():
        raise ValueError("{} is not a directory.".format(directory))

    total_bytes = 0
    directories_to_walk = [directory.resolve()]

    while directories_to_walk:
        next_directory = directories_to_walk.pop()

        for file_ in next_directory.iterdir():

            if file_.is_symlink():
                continue

            if file_.is_dir():
                directories_to_walk.append(file_)

            elif file_.is_file():
                total_bytes += file_.stat().st_size

    return total_bytes


def calculate_file_size(file_: pathlib.Path):
    if not (file_.is_dir() or (file_.is_file() and not file_.is_symlink())):
        raise ValueError("file_ must be a directory or a regular file.")

    if file_.is_dir():
        return calculate_directory_size(file_)

    if file_.is_file():
        return file_.stat().st_size


def compress_files(
    file_names: List[pathlib.Path], compressed_file_path: pathlib.Path
):
    """
    file_names must only contain regular files and/or directories.
    """
    if any([f.is_symlink() for f in file_names]):
        raise ValueError("file_names must not contain a symlink")

    try:
        _compress_files(file_names, compressed_file_path)
    except:
        os.remove(compressed_file_path)
        raise


def _compress_files(
    file_names: List[pathlib.Path], compressed_file_path: pathlib.Path
):
    tar = tarfile.open(name=str(compressed_file_path), mode="w:gz")

    timed_print("Calculating total size of files to compress ...")
    total_bytes_to_compress = sum(map(calculate_file_size, file_names))
    timed_print(
        f"Total bytes to compress is {human_readable_bytes(total_bytes_to_compress)}"
    )
    total_bytes_compressed = 0

    files_to_compress = file_names
    while files_to_compress:

        file_name = files_to_compress.pop()
        tarinfo = tar.gettarinfo(str(file_name))

        if tarinfo.isreg():
            file_size = file_name.stat().st_size
            timed_print(
                f"Compressing file {file_name} "
                f"[{human_readable_bytes(file_size)}] ..."
            )
            with open(file_name, "rb") as file_obj:
                tar.addfile(tarinfo, file_obj)
            total_bytes_compressed += file_size
            timed_print(
                "{:.2%} complete ({} of {} bytes compressed)".format(
                    total_bytes_compressed / total_bytes_to_compress,
                    human_readable_bytes(total_bytes_compressed),
                    human_readable_bytes(total_bytes_to_compress),
                )
            )

        elif tarinfo.isdir():
            tar.addfile(tarinfo)
            for sub_file in file_name.iterdir():
                files_to_compress.append(sub_file)

        else:
            tar.addfile(tarinfo)

    tar.close()
    compressed_file_size = compressed_file_path.stat().st_size
    timed_print(
        f"Compression complete. "
        f"Compressed {human_readable_bytes(total_bytes_to_compress)} "
        f"to {human_readable_bytes(compressed_file_size)} "
        + "(original size reduced to {:.2%})".format(
            compressed_file_size / total_bytes_to_compress
        )
    )


def upload_part(
    byte_pos,
    vault_name,
    upload_id,
    part_size,
    fileobj,
    file_size,
    num_parts,
    glacier,
):
    fileblock.acquire()
    fileobj.seek(byte_pos)
    part = fileobj.read(part_size)
    fileblock.release()

    range_header = "bytes {}-{}/{}".format(
        byte_pos, byte_pos + len(part) - 1, file_size
    )
    part_num = byte_pos // part_size
    percentage = part_num / num_parts

    timed_print(
        "Uploading part {0} of {1}... ({2:.2%})".format(
            part_num + 1, num_parts, percentage
        )
    )

    for i in range(MAX_ATTEMPTS):
        try:
            response = glacier.upload_multipart_part(
                vaultName=vault_name,
                uploadId=upload_id,
                range=range_header,
                body=part,
            )
            checksum = calculate_tree_hash(part, part_size)
            if checksum != response["checksum"]:
                timed_print("Checksums do not match. Will try again.")
                continue

            # if everything worked, then we can break
            break
        except:
            timed_print("Upload error: {}".format(sys.exc_info()))
            timed_print("Trying again. Part {0}".format(part_num + 1))
    else:
        timed_print("After multiple attempts, still failed to upload part")
        timed_print("Exiting.")
        sys.exit(1)

    del part
    return checksum


def upload(
    vault_name: str,
    file_to_upload: typing.io.BinaryIO,
    region: str,
    arc_desc: str,
    part_size: int,
    num_threads: int,
    upload_id: str,
):
    glacier = boto3.client("glacier", region)

    if not math.log2(part_size).is_integer():
        raise ValueError("part-size must be a power of 2")
    if part_size < 1 or part_size > 4096:
        raise ValueError(
            "part-size must be more than 1 MB " "and less than 4096 MB"
        )

    part_size = part_size * 1024 * 1024

    file_size = file_to_upload.seek(0, 2)
    file_to_upload.seek(0)

    if file_size < 4096:
        timed_print("File size is less than 4 MB. Uploading in one request...")

        response = glacier.upload_archive(
            vaultName=vault_name,
            archiveDescription=arc_desc,
            body=file_to_upload,
        )

        timed_print("Uploaded.")
        timed_print("Glacier tree hash: %s" % response["checksum"])
        timed_print("Location: %s" % response["location"])
        timed_print("Archive ID: %s" % response["archiveId"])
        timed_print("Done.")
        file_to_upload.close()
        return

    job_list = []
    list_of_checksums = []

    if upload_id is None:
        timed_print("Initiating multipart upload...")
        response = glacier.initiate_multipart_upload(
            vaultName=vault_name,
            archiveDescription=arc_desc,
            partSize=str(part_size),
        )
        upload_id = response["uploadId"]

        for byte_pos in range(0, file_size, part_size):
            job_list.append(byte_pos)
            list_of_checksums.append(None)

        num_parts = len(job_list)
        timed_print(
            "File size is {} bytes. Will upload in {} parts.".format(
                file_size, num_parts
            )
        )
    else:
        timed_print("Resuming upload...")

        timed_print("Fetching already uploaded parts...")
        response = glacier.list_parts(vaultName=vault_name, uploadId=upload_id)
        parts = response["Parts"]
        part_size = response["PartSizeInBytes"]
        while "Marker" in response:
            timed_print("Getting more parts...")
            response = glacier.list_parts(
                vaultName=vault_name,
                uploadId=upload_id,
                marker=response["Marker"],
            )
            parts.extend(response["Parts"])

        for byte_pos in range(0, file_size, part_size):
            job_list.append(byte_pos)
            list_of_checksums.append(None)

        num_parts = len(job_list)
        with click.progressbar(parts, label="Verifying uploaded parts") as bar:
            for part_data in bar:
                byte_start = int(part_data["RangeInBytes"].partition("-")[0])
                file_to_upload.seek(byte_start)
                part = file_to_upload.read(part_size)
                checksum = calculate_tree_hash(part, part_size)

                if checksum == part_data["SHA256TreeHash"]:
                    job_list.remove(byte_start)
                    part_num = byte_start // part_size
                    list_of_checksums[part_num] = checksum

    timed_print("Spawning threads...")
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num_threads
    ) as executor:
        futures_list = {
            executor.submit(
                upload_part,
                job,
                vault_name,
                upload_id,
                part_size,
                file_to_upload,
                file_size,
                num_parts,
                glacier,
            ): job
            // part_size
            for job in job_list
        }
        done, not_done = concurrent.futures.wait(
            futures_list, return_when=concurrent.futures.FIRST_EXCEPTION
        )
        if len(not_done) > 0:
            # an exception occured
            for future in not_done:
                future.cancel()
            for future in done:
                e = future.exception()
                if e is not None:
                    timed_print("Exception occured: %r" % e)
            timed_print("Upload not aborted. Upload id: %s" % upload_id)
            timed_print("Exiting.")
            file_to_upload.close()
            sys.exit(1)
        else:
            # all threads completed without raising
            for future in done:
                job_index = futures_list[future]
                list_of_checksums[job_index] = future.result()

    if len(list_of_checksums) != num_parts:
        timed_print("List of checksums incomplete. Recalculating...")
        list_of_checksums = []
        for byte_pos in range(0, file_size, part_size):
            part_num = int(byte_pos / part_size)
            timed_print("Checksum %s of %s..." % (part_num + 1, num_parts))
            file_to_upload.seek(byte_pos)
            part = file_to_upload.read(part_size)
            list_of_checksums.append(calculate_tree_hash(part, part_size))

    total_tree_hash = calculate_total_tree_hash(list_of_checksums)

    timed_print("Completing multipart upload...")
    response = glacier.complete_multipart_upload(
        vaultName=vault_name,
        uploadId=upload_id,
        archiveSize=str(file_size),
        checksum=total_tree_hash,
    )
    timed_print("Upload successful.")
    timed_print("Calculated total tree hash: %s" % total_tree_hash)
    timed_print("Glacier total tree hash: %s" % response["checksum"])
    timed_print("Location: %s" % response["location"])
    timed_print("Archive ID: %s" % response["archiveId"])
    timed_print("Done.")
    file_to_upload.close()


@click.command()
@click.option(
    "-v",
    "--vault-name",
    required=True,
    help="The name of the vault to upload to",
)
@click.option(
    "-f",
    "--file-name",
    required=True,
    multiple=True,
    help="The file or directory name on your local " "filesystem to upload",
)
@click.option(
    "--do-not-compress",
    is_flag=True,
    help=(
        "Don't compress file before uploading. "
        + "Requires --file-name to be a regular file"
    ),
)
@click.option("-r", "--region", help="The name of the region to upload to")
@click.option(
    "-d",
    "--arc-desc",
    default="",
    metavar="ARCHIVE_DESCRIPTION",
    help="The archive description to help identify archives later",
)
@click.option(
    "-p",
    "--part-size",
    type=int,
    default=8,
    help="The part size for multipart upload, in "
    "megabytes (e.g. 1, 2, 4, 8) default: 8",
)
@click.option(
    "-t",
    "--num-threads",
    type=int,
    default=5,
    help="The amount of concurrent threads (default: 5)",
)
@click.option(
    "-u",
    "--upload-id",
    help="Optional upload id, if provided then will " "resume upload.",
)
def upload_command(
    vault_name: str,
    file_name: List[str],
    do_not_compress: bool,
    region: str,
    arc_desc: str,
    part_size: int,
    num_threads: int,
    upload_id: str,
):
    file_names = file_name
    file_name_paths = list(map(pathlib.Path, file_names))

    if any([f.is_symlink() for f in file_name_paths]):
        raise ValueError("--file-name can not be a symlink.")

    if do_not_compress and len(file_name_paths) > 1:
        raise ValueError(
            "There may only be single --file-name when "
            + "--do-not-compress is specified."
        )
    if do_not_compress and not is_regular_file(file_name_paths[0]):
        raise ValueError(
            "Since --do-not-compress is specified, --file-name must be a "
            + "regular file."
        )

    file_path_to_upload = None
    if do_not_compress:
        file_path_to_upload = file_name_paths[0]
    else:
        compressed_file_path = pathlib.Path(
            os.getcwd()
        ) / "glacier_archive_created_{}.tar.gz".format(
            datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
        )
        compress_files(file_name_paths, compressed_file_path)
        file_path_to_upload = compressed_file_path

    with open(file_path_to_upload, "rb") as file_to_upload:
        upload(
            vault_name,
            file_to_upload,
            region,
            arc_desc,
            part_size,
            num_threads,
            upload_id,
        )

    if do_not_compress:
        pass
    else:
        # Remove the compressed file
        os.remove(file_path_to_upload)


if __name__ == "__main__":
    upload()
