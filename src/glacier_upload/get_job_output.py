import json
import pathlib
import tarfile

import boto3
import click


def decompress_file(compressed_file: pathlib.Path, results_dir: pathlib.Path):
    click.echo(f"Decompressing file {compressed_file} ...")
    with tarfile.open(compressed_file) as tar:
        tar.extractall(path=results_dir)


def get_job_output(vault_name, job_id, file_name):
    glacier = boto3.client("glacier")

    click.echo("Checking job status...")
    response = glacier.describe_job(vaultName=vault_name, jobId=job_id)

    click.echo("Job status: {}".format(response["StatusCode"]))

    if not response["Completed"]:
        click.echo("Exiting.")
        return
    else:
        click.echo("Retrieving job data...")
        response = glacier.get_job_output(vaultName=vault_name, jobId=job_id)

        if response["contentType"] == "application/json":
            inventory_json = json.loads(response["body"].read().decode("utf-8"))
            click.echo(json.dumps(inventory_json, indent=2))
        elif response["contentType"] == "text/csv":
            click.echo(response["body"].read())
        else:
            with open(file_name, "xb") as file:
                file.write(response["body"].read())

    file_name_path = pathlib.Path(file_name)
    decompress_file(file_name_path, file_name_path.parent)
    # TODO: delete compressed file


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
    default="glacier_archive.tar.gz",
    help="File name of archive to be saved, "
    "if the job is an archive-retrieval",
)
def get_job_output_command(vault_name, job_id, file_name):
    return get_job_output(vault_name, job_id, file_name)
