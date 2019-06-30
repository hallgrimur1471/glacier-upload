#!/usr/bin/env python3

import os
import pathlib
import shutil
import random
import sys
import filecmp

project_dir = pathlib.Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_dir / "src/glacier_upload"))
from upload import compress_files
from get_job_output import decompress_file

TEST_SPACE = pathlib.Path("/tmp/glacier_upload")


def main():
    try:
        prepare_test_space()
        run_test()
    finally:
        clean_up_test_space()


def prepare_test_space():
    print("Preparing test space ...")
    os.makedirs(TEST_SPACE)

    files_to_compress_dir = TEST_SPACE / "files_to_compress"
    os.mkdir(files_to_compress_dir)
    with open(files_to_compress_dir / "a.txt", "w") as f:
        for _ in range(10 * (10 ** 4)):
            f.write(str(random.randint(1000, 9999)))
    os.mkdir(files_to_compress_dir / "d")
    with open(files_to_compress_dir / "d/b.txt", "w") as f:
        for _ in range(2 * (10 ** 4)):
            f.write(str(random.randint(1000, 9999)))
    with open(files_to_compress_dir / "d/c.txt", "w") as f:
        for _ in range(2 * (10 ** 4)):
            f.write(str(random.randint(1000, 9999)))


def run_test():
    print("Running test ...\n")

    files_to_compress_dir = TEST_SPACE / "files_to_compress"
    os.chdir(str(files_to_compress_dir))
    files_to_compress = [pathlib.Path("a.txt"), pathlib.Path("d")]
    print("files to compress:", files_to_compress)

    compressed_file = compress_files(files_to_compress)

    result_file_dir = TEST_SPACE / "results"
    os.mkdir(result_file_dir)
    result_file = result_file_dir / "glacier_archive.tar.gz"
    with open(str(result_file), "wb") as rf:
        compressed_file.seek(0)
        shutil.copyfileobj(compressed_file, rf)

    assert result_file.stat().st_size > 0
    print()

    # TODO: programatically verify the following:

    directory_comparison = filecmp.dircmp(
        str(files_to_compress_dir), str(result_file_dir)
    )
    print(directory_comparison.report_full_closure())

    print()
    decompress_file(result_file, result_file_dir)
    os.remove(result_file)
    print()

    directory_comparison = filecmp.dircmp(
        str(files_to_compress_dir), str(result_file_dir)
    )
    print(directory_comparison.report_full_closure())

    print()


def clean_up_test_space():
    return
    print("Cleaning up test space ...")
    if TEST_SPACE.exists():
        shutil.rmtree(TEST_SPACE)


if __name__ == "__main__":
    main()
