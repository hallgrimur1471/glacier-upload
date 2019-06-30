import json
import pathlib
import tarfile
import datetime
import os

import boto3
import click


def timed_print(msg):
    now = datetime.datetime.now()
    hms_string = f"{now.strftime('%H:%M:%S')}.{now.strftime('%f')[0:3]}"
    click.echo(f"[{hms_string}] {msg}")


def decompress_file(compressed_file: pathlib.Path, results_dir: pathlib.Path):
    timed_print(f"Decompressing file {compressed_file} ...")
    with tarfile.open(compressed_file) as tar:
        tar.extractall(path=results_dir)
    timed_print("Decompressing successful")


def get_job_output(
    vault_name: str, job_id: str, compressed_file_path: pathlib.Path
):
    glacier = boto3.client("glacier")

    click.echo("Checking job status...")
    response = glacier.describe_job(vaultName=vault_name, jobId=job_id)

    click.echo("Job status: {}".format(response["StatusCode"]))

    if not response["Completed"]:
        click.echo("Exiting.")
        return False
    else:
        click.echo("Retrieving job data...")
        response = glacier.get_job_output(vaultName=vault_name, jobId=job_id)

        if response["contentType"] == "application/json":
            inventory_json = json.loads(response["body"].read().decode("utf-8"))
            click.echo(json.dumps(inventory_json, indent=2))
        elif response["contentType"] == "text/csv":
            click.echo(response["body"].read())
        else:
            with open(str(compressed_file_path), "xb") as file:
                file.write(response["body"].read())
        return True


@click.command()
@click.option(
    "-v",
    "--vault-name",
    required=True,
    help="The name of the vault to upload to",
)
@click.option("-j", "--job-id", required=True, help="Job ID")
@click.option(
    "-f",
    "--file-name",
    default="glacier_archive_retrieved_%Y_%m_%d_%H_%M_%S_%f.tar.gz",
    help="File name of archive to be saved, "
    "if the job is an archive-retrieval",
)
def get_job_output_command(vault_name, job_id, file_name):
    compressed_file_path = pathlib.Path(
        "glacier_archive_retrieved_{}.tar.gz".format(
            datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
        )
    )
    if get_job_output(vault_name, job_id, compressed_file_path):
        decompress_file(compressed_file_path, compressed_file_path.parent)
        os.remove(str(compressed_file_path))
