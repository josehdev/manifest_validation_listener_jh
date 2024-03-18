#!/usr/bin/env python3

import configparser
import csv
import gzip
import json
import jsonschema
import logging
import mysql.connector
import os
import pandas as pd
import pika
import requests
import shutil
import smtplib
import sys
import textwrap
import traceback
import uuid
import asyncio
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from email.message import EmailMessage
#from google.cloud import storage
from time import sleep
from jinja2 import Environment, FileSystemLoader
from typing import Dict
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters
from msgraph import GraphServiceClient

storage = {'bucket':'dummy'}

sys.path.append("/local/projects-t3/NEMO/bin/ingest_scripts/lib")

# Read config file
conf_loc = os.path.join(os.path.dirname(__file__), 'conf.ini')

if not os.path.isfile(conf_loc):
    sys.exit("Config file could not be found at {}".format(conf_loc))

config = configparser.ConfigParser()
config.read(conf_loc)

# RabbitMQ credentials for publisher and consumer
rabbitmq_credentials = pika.PlainCredentials(
    username=config['rabbitmq']['username'],
    password=config['rabbitmq']['password']
)


def append_validated_info(manifest_rows, submitter_email, submitter_username,
                          submission_id, vocab_to_dirname_mapping):
    """
    Appends the submission's validated information to each manifest row.
    """
    sub_logger.info("Appending validation content to manifest rows.")

    for row in manifest_rows:
        row["Contact_email"] = submitter_email
        row["Aspera_user"] = submitter_username
        row["Aspera_dir"] = submission_id
        row["Validated_nemo_dir"] = build_nemo_dir_from_metadata(
            row, vocab_to_dirname_mapping
        )

    return manifest_rows


def build_aspera_command(aspera_user, aspera_dir):
    """
    Build the aspera upload command to send to users.
    """
    sub_logger.debug("In build_aspera_command().")

    max_bandwidth = "1000M"
    aspera_server = "aspera.nemoarchive.org"
    cmd = "ascp -l {} -k 2 -QT <directory_of_files> {}@{}:{}/"

    return cmd.format(max_bandwidth, aspera_user, aspera_server, aspera_dir)


def build_nemo_dir_from_metadata(manifest_row, vocab_to_dirname_mapping):
    """
    Build a NeMO-specific directory path from the mapped metadata.
    """

    #[Access]/validated/brain/[Program]/grant/[Sub-program]/[Lab]/[Modality]/[Subspecimen_type]/[Technique]/[Species]/[Data_type]/

    DIR_ORDER = [
        "Sub-program", "Lab", "Modality", "Subspecimen_type", "Technique",
        "Species", "Data_type"
    ]

    # These two categories go before "grant"
    access = manifest_row["Access"].lower()

    if access not in vocab_to_dirname_mapping["Access"]:
        msg = f"Cannot map value '{access}' from key 'Access' " + \
              "in manifest file to a directory name. Skipping."
        raise KeyError(msg)

    access_dirname = vocab_to_dirname_mapping["Access"][access]

    program = manifest_row["Program"].lower()

    if program not in vocab_to_dirname_mapping["Program"]:
        msg = f"Cannot map value '{program}' from key 'Program' "  + \
              "in manifest file to a directory name. Skipping."
        raise KeyError(msg)
    program_dirname = vocab_to_dirname_mapping["Program"][program]

    nemo_dir = os.path.join(
        access_dirname, "validated", "brain", program_dirname, "grant"
    )

    # All of these go after "grant"
    for cat in DIR_ORDER:
        cat_vocab = manifest_row[cat].lower()

        if cat_vocab not in vocab_to_dirname_mapping[cat]:
            msg = f"Cannot map value '{cat_vocab}' from key '{cat}' " + \
                  "in manifest file to a directory name. Skipping."
            raise KeyError(msg)

        cat_dirname = vocab_to_dirname_mapping[cat][cat_vocab]
        nemo_dir = os.path.join(nemo_dir, cat_dirname)

    return nemo_dir


def process_message(body):
    """
    Process the message received from RabbitMQ.
    """
    logger.info("##################################################")
    logger.debug("In process_message().")

    data = None

    try:
        if message_well_formed(body):
            logger.debug("RabbitMQ message was well formed.")
            decoded_str = body.decode("utf-8")

            data = json.loads(decoded_str)
        else:
            logger.warning("Message wasn't well formed. Skipping.")
            return
    except Exception as err:
        logger.exception(f"Problem with message format. Reason: {err}")
        return

    formatted_msg = json.dumps(data, indent=2)
    logger.info(f"Message received from RabbitMQ:\n{formatted_msg}.")


    submission_id = data['submission_id']

    # Create the logger for this submission
    global sub_logger
    sub_logger = setup_submission_log(submission_id)

    logger.info("Start message processing.")
    handle_valid_message(data)

    logger.info("Completed message processing.")

    logger.info("Done.")
    logger.info("##################################################")


# Get submissions's container name
def get_submission_container_name(submission_id, access_level):
    """
    Returns the submission container's name.
    """
    sub_logger.debug("In get_submission_container_name().")

    if access_level == "restricted":
        access_type = "restricted"
    else:
        access_type = "public"

    return f'{access_type}-{submission_id}'


def create_submission_container(blob_service_client, container_name):
    """
    Create a blob container for the submitted data.
    """
    sub_logger.debug("In create_submission_container().")

    # Create container for the submission
    try:
        blob_service_client.create_container(container_name)
    except Exception as err:
        sub_logger.exception(f"Error in create container {container_name}: {err}",
                             exc_info=True)


def assign_permissions_to_container(cloud_storage_config, container_name, submitter_email):
    """
    Assigns Blob Contributor role into container
    """
    sub_logger.debug("In assign_permissions_to_container().")

    # Get authorization client
    authorization_client = get_authorization_client(cloud_storage_config)

    # Get graph service client
    graph_service_client = get_graph_service_client(cloud_storage_config)

    # Create the scope for the role assignment 
    subscription = f'subscriptions/{cloud_storage_config['azure-subscription-id']}'
    res_group = f'resourceGroups/{cloud_storage_config['azure-resource-group']}'
    storage_acc = f'providers/Microsoft.Storage/storageAccounts/{cloud_storage_config['azure-storage-account']}'
    container = f'blobServices/default/containers/{container_name}'
    scope = f'{subscription}/{res_group}/{storage_acc}/{container}'

    # Create the role definition string 
    role_assignment_name = uuid.uuid4() # random guid
    role_id = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe' # Storage Blob Data Contributor
    role_template = '/subscriptions/{}/providers/Microsoft.Authorization/roleDefinitions/{}'
    role_definition_id = role_template.format(cloud_storage_config['azure-subscription-id'],role_id)

    # Get user id of guest user account
    user = asyncio.run(get_azure_user_by_email(graph_service_client, submitter_email))
    principal_id = user.id

    # Create the role assignments parameters
    role_assignment_params = RoleAssignmentCreateParameters(
        principal_id = principal_id,
        role_definition_id = role_definition_id
    )

    try:
        authorization_client.role_assignments.create(
            scope,
            role_assignment_name,
            role_assignment_params
        )
        
        sub_logger.info("Created Role Assignment.")

    except Exception as err:
        if 'RoleAssignmentExists' in err.message:
            sub_logger.warning("Role Assignment already exists. Ignore.")
        else: 
            raise err


def determine_manifest_access_level(manifest_row):
    """
    Returns the access level of a given row from a validated manifest.
    """
    access_level = None

    if manifest_row['Access'] == "open":
        access_level = "open"
    elif manifest_row['Access'] == "embargo":
        access_level = "embargo"
    elif manifest_row['Access'] == "controlled":
        access_level = "restricted"

    return access_level


def determine_access_level_from_excel_manifest(manifest_filepath):
    """
    Reads the access level from a validated manifest excel file.
    """
    access_level = None

    try:
        file_df = pd.read_excel(manifest_filepath,
            sheet_name='file',
            usecols=['access']
        )
        access_level = file_df['access'].iloc[0]
    except Exception as err:
        sub_logger.exception("Error occurred while getting access_level" + \
                            f" from manifest {manifest_filepath}: {err}",
                            exc_info=True)

    # Change "open_embargo" and "restricted_embargo" to "embargo"
    if "embargo" in access_level:
        access_level = "embargo"

    return access_level


def download_errors(error_gcp_path, cloud_storage_config):
    """
    Download the errors file from GCP to the filesystem.
    """
    sub_logger.debug("In download_errors().")

    error_bucket = cloud_storage_config['error-bucket']
    project_id = cloud_storage_config['project-id']
    submitted_manifests_root = config['general']['submitted-manifests-root']

    storage_client = storage.Client.from_service_account_json(
        cloud_storage_config['credentials']
    )
    bucket_client = storage_client.bucket(error_bucket, project_id)
    blob = bucket_client.blob(os.path.basename(error_gcp_path))

    error_location = os.path.join(
        submitted_manifests_root,
        os.path.basename(error_gcp_path)
    )

    blob.download_to_filename(error_location)

    sub_logger.info(f"Errors downloaded to local filesystem: {error_location}")

    return error_location

## TODO: remove this function
def download_manifest_old(manifest_path, manifest_id, file_extension,
                      cloud_storage_config):
    """
    Download the manifest from GCP to the filesystem.
    """
    sub_logger.debug("In download_manifest().")

    manifest_bucket = cloud_storage_config['manifest-bucket']
    project_id = cloud_storage_config['project-id']
    submitted_manifests_root = config['general']['submitted-manifests-root']

    storage_client = storage.Client.from_service_account_json(
        cloud_storage_config['credentials']
    )
    bucket_client = storage_client.bucket(manifest_bucket, project_id)
    blob = bucket_client.blob(os.path.basename(manifest_path))

    new_manifest_loc = os.path.join(
        submitted_manifests_root,
        f"{manifest_id}_manifest.{file_extension}"
    )

    blob.download_to_filename(new_manifest_loc)

    sub_logger.info(f"Manifest downloaded to: {new_manifest_loc}")

    return new_manifest_loc


def download_manifest(manifest_path, manifest_id, file_extension,
                      cloud_storage_config, blob_service_client):
    """
    Download the manifest from GCP to the filesystem.
    """
    sub_logger.debug("In download_manifest().")

    blob_name = os.path.basename(manifest_path)
    blob = get_blob_client(blob_service_client, cloud_storage_config['manifest-container'], blob_name)

    submitted_manifests_root = config['general']['submitted-manifests-root']

    new_manifest_loc = os.path.join(
        submitted_manifests_root,
        f"{manifest_id}_manifest.{file_extension}"
    )

    with open(new_manifest_loc, mode="wb") as file:
        stream = blob.download_blob()
        file.write(stream.readall())

    sub_logger.info(f"Manifest downloaded to: {new_manifest_loc}")

    return new_manifest_loc


def get_authorization_client(cloud_storage_config):
    """
    Creates and returns an authorization client authenticated with Azure.
    """    
    appCredential = ClientSecretCredential(
        tenant_id = cloud_storage_config['azure-tenant-id'], 
        client_id = cloud_storage_config['azure-client-id'],   
        client_secret= cloud_storage_config['azure-client-secret']
    )

    authorization_client = AuthorizationManagementClient(
        appCredential,
        cloud_storage_config['azure-subscription-id']
    )

    return authorization_client


def get_graph_service_client(cloud_storage_config):
    """
    Creates and returns a graph service client authenticated with Azure.
    """    
    appCredential = ClientSecretCredential(
        tenant_id = cloud_storage_config['azure-tenant-id'], 
        client_id = cloud_storage_config['azure-client-id'],   
        client_secret= cloud_storage_config['azure-client-secret']
    )

    scopes = ['https://graph.microsoft.com/.default']

    graph_service_client = GraphServiceClient(appCredential, scopes)

    return graph_service_client


def get_blob_service_client(cloud_storage_config):
    """
    Creates and returns a Blob Service client authenticated with Azure.
    """    
    # TODO: change this credential to client/secret credential
    credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient.from_connection_string(
            cloud_storage_config['azure-storage-connection-string'], 
            credential
    )

    return blob_service_client


def get_blob_client(client: BlobServiceClient, ct_name: str, blob_name: str):
    """
    Returns a Blob Client for the specified blob name.
    """    
    container_client = client.get_container_client(ct_name)
    blob_client = container_client.get_blob_client(blob_name)

    return blob_client


async def get_azure_user_by_email(graph_service_client: GraphServiceClient, email: str):
    """
    Finds and returns an Azure user object, by its email address.
    """    
    email = email.lower()
    users = await graph_service_client.users.get()
    user = next((u for u in users.value if u.mail and u.mail.lower() == email), None)
    next_link = users.odata_next_link

    while not user and next_link:
        users = await graph_service_client.users.with_url(next_link).get()
        user = next((u for u in users.value if u.mail and u.mail.lower() == email), None)
        next_link = users.odata_next_link

    return user


def get_admin_emails():
    """
    Parse the configuration file for the email addresses of the NeMO
    administrators that should be notified when ingest events occur.
    """
    sub_logger.debug("In get_admin_emails().")

    admin_email_recipients = config['email']['admins']
    admin_email_recipients = admin_email_recipients.strip()
    admin_email_recipients = admin_email_recipients.split(",")

    return admin_email_recipients


def get_email_template():
    """
    Load and retrieve the template used for outbound HTML email.
    """
    sub_logger.debug("In get_email_template().")

    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')

    env = Environment(loader = FileSystemLoader(templates_dir))

    template = env.get_template('template.html')

    return template


def get_listener_version():
    """
    Reads the listener's VERSION file and returns the version as string.
    """
    logger.debug("In get_listener_version().")

    self_dir = os.path.dirname(os.path.abspath(__name__))
    version_path = os.path.join(self_dir, "VERSION")

    try:
        with open(version_path, "r") as fh:
            version_text = fh.read()

            return version_text
    except:
        sys.stderr.write(
            f"Unable to load listener version at {version_path}.\n"
        )
        sys.exit(1)


def get_submission_docroot(username):
    """
    Returns the Aspera submission docroot for a given NeMO username.
    """
    sub_logger.debug("In get_submission_docroot().")
    nemo_internal_root = config['general']['nemo-internal-root-url']

    endpoint = f"{nemo_internal_root}/users/{username}/submission-docroot"

    response = requests.get(endpoint)

    if response.status_code == 200:
        sub_logger.debug("Submitter docroot retrieved.")
        data = json.loads(response.text)
        return data['docroot']
    else:
        sub_logger.error("Unable to lookup submitter information.")
        raise Exception("Unable to lookup submitter information.")


def get_rabbitmq_channel(rabbitmq_connection, exchange_name, queue_name,
                         routing_key):
    """
    Returns a RabbitMQ pika.channel.Channel
    """
    logger.debug("In get_rabbitmq_channel().")

    channel = rabbitmq_connection.channel()

    channel.exchange_declare(exchange=exchange_name,
                             exchange_type="direct",
                             durable=True)
    channel.queue_declare(queue=queue_name,
                          durable=True,
                          arguments={"x-single-active-consumer": True})

    # Establish relationship between exchange and queue
    channel.queue_bind(exchange=exchange_name,
                       queue=queue_name,
                       routing_key=routing_key)

    return channel


def get_rows_from_manifest(manifest_filepath):
    """
    Read submitted manifest file using 'csv' module and return list of dicts
    for each row.
    """
    sub_logger.info("Reading CSV or TSV manifest submission.")

    # Choose delimiter based on extension.
    delimiter = ','
    if os.path.splitext(manifest_filepath)[1] in ['.tab', '.tsv', '.txt']:
        delimiter = '\t'

    rows = []
    with open(manifest_filepath, encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=delimiter,
                                    dialect='excel')

        # Row is a dictionary of headers => vals for that row
        for row in csv_reader:
            rows.append(row)

    return rows


def handle_valid_message(data):
    """
    Handles correctly formatted and valid messages from GCP.
    """
    sub_logger.debug("In handle_valid_message().")

    submission_id = data['submission_id']
    manifest_path = data['manifest_path']
    project_name = data['project_name']
    program = data['program']
    submitter_email = data['submitter']['email']
    submitter_username = data['submitter']['username']

    # Dry run is optional. If present grab the value, otherwise assume False
    # Manifest validation cloud function provides a boolean True or False
    # value
    if 'dryrun' in data:
        dryrun = data['dryrun']
    else:
        dryrun = False

    sub_logger.info(f"Submission is a dryrun: {dryrun}")

    # Determine what GCP project we need to connect to
    if project_name == "scorch":
        # SCORCH program has its' own GCP project
        cloud_storage_config = config['storage-scorch']
        manifest_extension = "xlsx"
    else:
        # All other programs use the NeMO GCP project
        cloud_storage_config = config['storage-nemo']
        manifest_extension = "tsv"

    sub_logger.info(f"Submission program is '{program}'.")

    # Get blob service client
    blob_service_client = get_blob_service_client(cloud_storage_config)

    # Download the manifest to IGS filesystem
    try:
        sub_logger.info("Downloading manifest from GCP to local filesystem")
        local_manifest_path = download_manifest(
            manifest_path,
            submission_id,
            manifest_extension,
            cloud_storage_config,
            blob_service_client
        )
    except Exception as err:
        error_message = "Unable to download validated manifest from " + \
                        f"{manifest_path} due to error: "
        sub_logger.exception(error_message, exc_info=True)
        error_message += "\n"
        error_message += traceback.format_exc()
        send_misc_error_email(submission_id, manifest_path, error_message, dryrun)
        return

    # Determine if the controlled vocab to directory name mapping file exists
    vocab_to_dirname_filepath = config['general']['dirname-mappings-filepath']
    if not os.path.isfile(vocab_to_dirname_filepath):
        msg = f"Vocab to directory name mapping file ({vocab_to_dirname_filepath}) " + \
              "could not be found."
        sub_logger.error(f"{msg}. Emailing NeMO admins.")
        send_misc_error_email(submission_id, manifest_path, msg, dryrun)
        return

    if data["result"] is True:
        sub_logger.info(f"Manifest {submission_id} " + \
                    "has been validated successfully.")

        try:
            if dryrun:
                send_dryrun_success_email(submission_id, data['submitter'])

            else:
                # Determine the filepath for validated manifest
                valid_manifests_root = config['general']['valid-manifests-root']
                validated_manifest_filepath = os.path.join(
                    valid_manifests_root,
                    f"{submission_id}_manifest.{manifest_extension}"
                )

                if program == 'bican':
                    # Copy manifest into valid manifest area.
                    # We are not appending info to BICAN manifests.
                    shutil.copy(local_manifest_path, validated_manifest_filepath)

                    # At this point in Ingest, all BICAN submissions are considered restricted
                    access_level = "restricted"
                elif program == 'scorch' and project_name == 'scorch':
                    # Copy manifest into valid manifest area.
                    # We are not appending info to SCORCH manifests.
                    shutil.copy(local_manifest_path, validated_manifest_filepath)

                    # Determine access_level
                    access_level = determine_access_level_from_excel_manifest(
                        local_manifest_path
                    )
                else:
                    # All other submissions
                    vocab_to_dirname_mapping = load_vocab_to_dirname_mapping(
                        vocab_to_dirname_filepath
                    )

                    submitted_manifest_rows = get_rows_from_manifest(local_manifest_path)

                    validated_manifest_rows = append_validated_info(
                        submitted_manifest_rows,
                        submitter_email,
                        submitter_username,
                        submission_id,
                        vocab_to_dirname_mapping
                    )

                    # Write validated manifest to file
                    write_validated_manifest(validated_manifest_filepath, validated_manifest_rows)

                    # Read 1st row of manifest to determine some basic metadata about the manifest
                    file_row = validated_manifest_rows[0]
                    access_level = determine_manifest_access_level(file_row)

                # Get submissions's container name
                container_name = get_submission_container_name(submission_id, access_level)
                
                # Create the blob container for the submission
                create_submission_container(blob_service_client, container_name)

                # Assign permissions to submission's container
                assign_permissions_to_container(cloud_storage_config, 
                                                container_name,
                                                submitter_email)

                update_submission_db(data, access_level)

                aspera_cmd = build_aspera_command(submitter_username, submission_id)

                send_success_email(submission_id, aspera_cmd, data['submitter'])
        except Exception as err:
            sub_logger.exception("Unexpected error occurred: ", exc_info=True)
            error_message = "Unexpected error occurred:\n"
            error_message += traceback.format_exc()
            send_misc_error_email(submission_id, local_manifest_path, error_message, dryrun)
    elif "complete_errors" not in data:
        sub_logger.info("Manifest could not be properly validated.")

        if "errors" in data:
            for error in data['errors']:
                sub_logger.warning(error)
            use_errors_email_body = True
        else:
            # Use corrupt file template
            use_errors_email_body = False

        if not dryrun:
            update_submission_db(data)

        send_failure_email(
            submission_id,
            data,
            dryrun,
            use_errors_email_body
        )
    elif data["complete_errors"] is True:
        # This will handle a scenario where there are few errors and all of them
        # are sent in the JSON message.
        sub_logger.info(f"Manifest {submission_id} did not validate successfully. Errors:")
        for error in data['errors']:
            sub_logger.warning(error)
        sub_logger.info(f"Errors file in {data['gcp_error_path']}")

        if not dryrun:
            update_submission_db(data)

        send_failure_email(
            submission_id,
            data,
            dryrun,
            True
        )
    elif data["complete_errors"] is False:
        # Handle when the errors in the json message are incomplete, send the
        # error file to the user
        sub_logger.info(f"Manifest {submission_id} failed to validate with many issues.")
        error_local_path = download_errors(
            data['gcp_error_path'],
            cloud_storage_config
        )
        data['errors'] = ["Manifest failed validation with many issues. " + \
                          "Please refer to email for attached error file."]

        if not dryrun:
            update_submission_db(data)

        send_full_failure_email(
            submission_id,
            data['submitter'],
            error_local_path,
            dryrun
        )

    sub_logger.info(f"Done with submission {submission_id}.")


def load_vocab_to_dirname_mapping(json_filepath):
    """
    Loads the manifest controlled vocabulary to directory name mappings from
    file.
    """
    sub_logger.debug("In load_vocab_to_dirname_mapping().")

    with open(json_filepath, "r") as fh:
        mapping_text = fh.read()

        return json.loads(mapping_text)


def load_schema():
    """
    Loads the JSON-Schema from a file into memory.
    """
    logger.debug("In load_schema().")

    self_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(self_dir, "validation_result_schema.json")

    try:
        with open(schema_path, "r") as fh:
            schema_text = fh.read()

            return json.loads(schema_text)
    except:
        sys.stderr.write(
            f"Unable to load JSON schema at {schema_path}.\n"
        )
        sys.exit(1)


def message_well_formed(message):
    """
    Check if the data from RabbitMQ for this listener is well-formed.
    """
    logger.debug("In message_well_formed().")

    well_formed = False

    data = None

    # Ensure we're receiving a valid JSON document.
    try:
        decoded_str = message.decode("utf-8")

        data = json.loads(decoded_str)

        # Use jsonschema to validate the structure of the document.
        jsonschema.validate(instance=data, schema=schema)

        well_formed = True
    except json.decoder.JSONDecodeError as err:
        # Invalid message. Acknowledge to clear it from RabbitMQ.
        msg = "Message contained invalid JSON: {}".format(str(message))
        logger.warning(msg)
    except jsonschema.exceptions.ValidationError as err:
        logger.warning(
            "JSON in message didn't conform to spec. " + \
            f"Reason: {err.message}"
        )

    return well_formed


def send_dryrun_success_email(submission_id, submitter):
    """
    Send an email to the user notifying them that the manifest successfully
    passed validation.
    """
    sub_logger.debug("In send_dryrun_success_email().")

    try:
        subject = "NeMO Archive - Dry Run - Valid manifest submission" + \
                  f" {submission_id}"

        text1 = """
        Hello {first},

        We'd like to inform you that the manifest you submitted successfully
        validated during a dry run!
        """

        text2 = """
        If you have any questions about the submission process, please consult
        the documentation at

        https://nemoarchive.org/resources/data-submission

        or contact us at nemo@som.umaryland.edu for assistance.
        """

        text1 = text1.format(first=submitter['first'])

        contact_email = submitter['email']
        to_list = [contact_email]

        send_email_wrapper(subject, submission_id, text1, text2, to_list)
    except Exception as err:
        sub_logger.exception("Error in sending dry run success email: ",
                              exc_info=True)


def send_success_email(submission_id, aspera_cmd, submitter):
    """
    Send an email to the user with the manifest and the aspera command to use.
    """
    sub_logger.debug("In send_success_email().")

    try:
        subject = f"NeMO Archive - Valid manifest for submission: {submission_id}"

        text1 = """
        Hello {first},

        We'd like to inform you that the manifest you submitted successfully
        validated. The submission ID is
        """

        text2 = """
        You are now free to begin submitting data to NeMO via Aspera.
        Please use the following command to upload your files:

        {aspera_cmd}

        It is important that the submission location path is correct, or
        these files may not be correctly associated with the submitted
        manifest.

        If you have any questions about the submission process, please consult
        the documentation at

        https://nemoarchive.org/resources/data-submission

        or contact us at nemo@som.umaryland.edu for assistance.
        """

        text1 = text1.format(first=submitter['first'])
        text2 = text2.format(aspera_cmd=aspera_cmd)

        contact_email = submitter['email']
        to_list = [contact_email]

        send_email_wrapper(subject, submission_id, text1, text2, to_list)
    except Exception as err:
        sub_logger.exception("Error in sending success email: ", exc_info=True)


def send_failure_email(submission_id, data, dryrun, use_errors_email_body):
    """
    Send an email to the submitter if there are 5 or fewer errors, with the
    manifest attached and the list of errors in the email body.
    """
    sub_logger.debug("In send_failure_email().")

    submitter = data['submitter']
    errors = data['errors']
    program = data['program']

    try:
        dryrun_txt = "- Dry Run -" if dryrun else "-"

        subject = f"NeMO Archive {dryrun_txt} Invalid manifest submission " + \
                  f"{submission_id}"

        text1 = """Hello {}!

        We regret that your submitted manifest did not pass validation{}.
        The submission ID was
        """

        text2 = """
        Corrections will be required before data can be uploaded via Aspera.

        {}

        If you have any questions about the correct values to use in your
        manifest, please consult the controlled vocabulary listed below:

        For BICCN Submissions:
        ----------------------
        https://docs.google.com/spreadsheets/d/1Z7h1_6Wgw8OurEoOAXU94yQcU5n0rLBK/

        For BICAN and other program submissions:
        ----------------------------------------
        contact nemo@som.umaryland.edu for assistance.
        """

        body_dryrun_text = " during a dry run" if dryrun else ""

        error_text = ""

        if use_errors_email_body:
            errors = ["\n\t- " + err for err in errors]
            error_list = "".join(errors)

            error_text = """The issues with your manifest were:

            {}
            """

            error_text = error_text.format(error_list)
            error_text = textwrap.dedent(error_text)

        text1 = text1.format(submitter['first'], body_dryrun_text)
        text2 = text2.format(error_text)

        # De-dent the body of the message
        text1 = textwrap.dedent(text1)
        text2 = textwrap.dedent(text2)

        contact_email = submitter['email']
        to_list = [contact_email]

        send_email_wrapper(subject, submission_id, text1, text2, to_list)
    except Exception as err:
        sub_logger.exception("Error in send_failure_email: ", exc_info=True)


def send_full_failure_email(submission_id, submitter, error_file, dryrun):
    """
    Email the submitter with the manifest errors as an attachment.
    This is done when the number of errors is too many to reasonably list
    in the body of the email. So, we gather all the errors and put them
    into an attachment.
    """
    sub_logger.debug("In send_full_failure_email().")

    first = submitter['first']

    try:
        dryrun_txt = "- Dry Run -" if dryrun else "-"
        subject = f"NeMO Archive {dryrun_txt} Invalid manifest submission " + \
                  f"{submission_id}"

        text1 = """
        Hello {first},

        We regret that your submitted manifest for submission ID
        """

        text2="""
        did not pass validation{dry}.

        Corrections will need to be made to it before it can be submitted
        and data transmitted to NeMO.

        A list of the issues with the manifest has been attached to this
        email.

        If you have any questions about the correct values to use in your
        manifest, please consult the controlled vocabulary listed below:

        For BICCN Submissions:
        ----------------------
        https://docs.google.com/spreadsheets/d/1Z7h1_6Wgw8OurEoOAXU94yQcU5n0rLBK/

        For BICAN and other program submissions:
        ----------------------------------------
        contact nemo@som.umaryland.edu for assistance.
        """

        body_dryrun_text = " during a dry run" if dryrun else ""

        text1 = text1.format(first=first)
        text2 = text2.format(dry=body_dryrun_text)

        contact_email = submitter['email']
        to_list = [contact_email]

        attachments = [error_file]

        send_email_wrapper(
            subject, submission_id, text1, text2, to_list, attachments
        )
    except Exception as e:
        sub_logger.exception(
            "Error in send_full_failure_email: ",
            exc_info=True
       )


def send_misc_error_email(submission_id, manifest, error, dryrun):
    """
    Send an error only to the NeMO admins to fix an internal error that occured.
    """
    sub_logger.debug("In send_misc_error_email().")

    text1 = """
    Hello,

    A manifest successfully passed validation, but a failure occurred
    while reading the manifest at '{}', or setting up the Aspera
    submission directory. We may need to:

    1. Create the Aspera directory.
    2. Update the submission's status in the submission database.
    3. Contact the submitter to provide an Aspera upload command.

    The following error occurred:

    {}

    See the log for more information: {}
    """

    try:
        dryrun_txt = "- Dry Run -" if dryrun else "-"
        subject = f"NeMO Archive {dryrun_txt} Manifest Validation Listener Error " + \
                  f"for Submission: {submission_id}"

        text1 = text1.format(manifest, error, logfile)
        text2 = ""

        admin_recipients = get_admin_emails()

        send_email_wrapper(subject, submission_id, text1, text2, admin_recipients)
    except Exception as err:
        sub_logger.exception("Error in send_misc_error_email: ", exc_info=True)


def send_email(subject, text, html, to_list, cc_list=None, attachment_filepaths=None):
    """
    Sends email. cc_list is empty when email is only going to NeMO Admins
    attachment_filepaths is empty, otherwise a list of full filepaths to attach
    to the email.
    """
    sub_logger.debug("In send_email().")

    ATTACHMENT_MAX = 5000000  # 5MB in bytes
    server = None

    try:
        message = EmailMessage()
        message.set_content(text)

        message.add_alternative(html, subtype="html")

        sender_email = config['email']['sender-email']
        sender_email = sender_email.strip()
        message['From'] = sender_email
        message['To'] = ', '.join(to_list)

        if cc_list:
            message['Cc'] = ', '.join(cc_list)

        message["Subject"] = subject

        if attachment_filepaths:
            for filepath in attachment_filepaths:
                # Attach the finished manifest
                filename = os.path.basename(filepath)

                # compressing the manifest if it is 3/4 of the way to the maximum size
                if os.path.getsize(filepath) > int(ATTACHMENT_MAX * 0.75):
                    with open(filepath, 'rb') as f:
                        bin_text = f.read()
                        attachment_text = gzip.compress(bin_text)
                        filename = filename + ".gz"
                        message.add_attachment(attachment_text,
                                               maintype='application',
                                               subtype='octet-stream',
                                               filename=filename)
                else:
                    with open(filepath, 'rb') as f:
                        attachment_text = f.read()
                        message.add_attachment(attachment_text,
                                               maintype='text',
                                               subtype='tab-separated-values',
                                               filename=filename)

        server = smtplib.SMTP('localhost')
        #server.set_debuglevel(1)

        server.send_message(message)
        sub_logger.info(f"Email sent to: {', '.join(to_list)}")
    except Exception as err:
        sub_logger.exception("Error in send_email: ", exc_info=True)
    finally:
        if server:
            server.quit()


def send_email_wrapper(subject: str, submission_id: str, text1: str, text2: str, to_list: list, attachments=None):
    """
    Send an email to the user notifying them that the manifest successfully
    passed validation.
    """
    sub_logger.debug("In send_email_wrapper().")

    now_dt = datetime.now()
    year = now_dt.strftime('%Y')

    try:
        plain_text = """
        {text1}

        {submission_id}

        {text2}
        """

        # De-dent the body of the message
        text1 = textwrap.dedent(text1)
        text2 = textwrap.dedent(text2)

        plain_text = plain_text.format(
            submission_id=submission_id,
            text1=text1,
            text2=text2
        )

        plain_text = textwrap.dedent(plain_text)

        htmlified1 = text1.replace('\n', '<br/>\n')
        htmlified2 = text2.replace('\n', '<br/>\n')

        data = {
            "submission_id": submission_id,
            "text1": htmlified1,
            "text2": htmlified2,
            "year": year
        }

        # Fill the template with the data we have created.
        template = get_email_template()
        html = template.render(data)

        admin_email_recipients = get_admin_emails()

        send_email(
            subject, plain_text, html, to_list, admin_email_recipients,
            attachments
        )
    except Exception as err:
        sub_logger.exception("Error in sending email notification: ",
                              exc_info=True)


def setup_log():
    """
    Set up log to rotate every Sunday
    """

    # Allows logfile to be included into admin email
    global logfile

    log_name = config['logger']['log-name']
    logfile = os.path.join(
        config['logger']['log-root-dir'],
        config['logger']['log-dir'],
        log_name + ".log"
    )
    log_rotate_when = config['logger']['rotate-when']

    logger = logging.getLogger(log_name)

    listener_log_level = config['logger']['listener-log-level']
    logger.setLevel(listener_log_level)

    if not len(logger.handlers):
        log_handler = TimedRotatingFileHandler(logfile, when=log_rotate_when)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)

        logger.addHandler(log_handler)
        logger.info("Log started!")
    else:
        logger.info("Reusing existing logger!")

    return logger


def setup_submission_log(submission_id):
    log_dir = os.path.join(config['logger']['log-root-dir'], config['logger']['log-dir'])
    first_char_submission_id = submission_id[0]

    log_dir_parts = [log_dir, first_char_submission_id]

    os.makedirs(os.sep.join(log_dir_parts), exist_ok=True)

    log_name = submission_id + ".log"
    full_log_path = os.path.join(log_dir, first_char_submission_id, log_name)

    message_logger = logging.getLogger(log_name)

    submission_log_level = config['logger']['submission-log-level']
    message_logger.setLevel(submission_log_level)

    if not len(message_logger.handlers):
        fh = logging.FileHandler(full_log_path)
        fh.setLevel(submission_log_level)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        message_logger.addHandler(fh)

        message_logger.info("Submission log started!")
    else:
        message_logger.info("Reusing existing submission logger!")

    return message_logger


def update_submission_db(data: dict, access_level=None):
    """
    Updates the submission database with the time, if the data is public or
    restricted and whether it succeeded and any errors.
    """
    sub_logger.debug("In update_submission_db().")

    sub_logger.info("Manifest access level: {}".format(str(access_level)))

    try:
        sub_logger.info("Connecting to submission tracking database")
        conn = mysql.connector.connect(
            user=config['db']['username'],
            passwd=config['db']['password'],
            host=config['db']['host'],
            database=config['db']['database']
        )

        # Update everything in the database in one big query
        cmd = """
        UPDATE submissions
        SET manifest_validated = %(manifest_validated)s,
        manifest_validated_dt = now(),
        manifest_validated_txt = %(manifest_validated_txt)s,
        policy = %(policy)s,
        program = %(program)s
        WHERE submission_id = %(submission_id)s
        """

        # Formulate the message that we put in the database and display to our
        # submitters.
        manifest_validated_txt = None

        if data['result']:
            manifest_validated_txt = "Manifest validated successfully."
        else:
            manifest_validated_txt = ",".join(data['errors'])

        params = {
            'manifest_validated': "success" if data['result'] else "error",
            'manifest_validated_txt': manifest_validated_txt,
            'policy': access_level,
            'program': data['program'],
            'submission_id': data['submission_id']
        }

        cursor = conn.cursor()

        cursor.execute(cmd, params)

        conn.commit()
    except mysql.connector.Error as err:
        # Rollback any changes
        conn.rollback()
        sub_logger.exception("Unable to update_submission_db: ", exc_info=True)
    finally:
        # Always close cursor and connection
        cursor.close()
        conn.close()


def write_validated_manifest(validated_manifest_filepath, rows):
    """
    Write a new manifest file using provided rows (row = dict).
    """
    sub_logger.info(f"Writing validated manifest to file: {validated_manifest_filepath}.")

    with open(validated_manifest_filepath, encoding='utf-8', mode='w') as csv_file:
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(csv_file, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()
        [writer.writerow(row) for row in rows]


def main():
    """
    Set up the RabbitMQ consumer.
    If the connection to RabbitMQ is broken, this automatically tries to
    reconnect after a configured wait time. If the connection cannot be
    restored after the configured number of attempts, the script exits.

    Source for reconnecting:
    https://pika.readthedocs.io/en/1.1.0/examples/blocking_consume_recover_multiple_hosts.html
    """
    logger.info("Setting up RabbmitMQ consumer.")

    pause_filepath = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                  "pause_listener")

    # Get queue and connection retry configuration
    exchange_name = config['rabbitmq']['consumer-exchange-name']
    queue_name = config['rabbitmq']['consumer-queue-name']
    routing_key = config['rabbitmq']['consumer-routing-key']
    max_reconnect_attempts = int(config['rabbitmq']['max-reconnect-attempts'])
    wait_time = int(config['rabbitmq']['reconnect-wait-time'])
    cxn_attempts = 1
    rabbitmq_connection = None
    rabbitmq_channel = None

    while True:
        try:
            if os.path.isfile(pause_filepath):
                logger.warning(
                    f"Detected semaphore pause file ({pause_filepath}). " + \
                    f"Pausing consumer for {wait_time} seconds..."
                )
                sleep(wait_time)
                continue

            if not rabbitmq_connection or rabbitmq_connection.is_closed:
                host = config['rabbitmq']['host']
                logger.info(f"Connecting to RabbitMQ at {host} (attempt #{cxn_attempts})...")

                # RabbitMQ parameters to consumer connection
                cxn_parameters = pika.ConnectionParameters(
                    host=host,
                    port=int(config['rabbitmq']['port']),
                    virtual_host=config['rabbitmq']['virtual-host'],
                    credentials=rabbitmq_credentials
                )

                # Get connection to RabbitMQ instance on GCP
                rabbitmq_connection = pika.BlockingConnection(
                    parameters=cxn_parameters
                )
                logger.info("Connected to RabbitMQ!")

            if not rabbitmq_channel or rabbitmq_channel.is_closed:
                # Get a connection channel.
                logger.info("Opening channel...")
                rabbitmq_channel = get_rabbitmq_channel(
                    rabbitmq_connection,
                    exchange_name,
                    queue_name,
                    routing_key
                )

                # Set max number of messages a consumer can pull at one time.
                rabbitmq_channel.basic_qos(prefetch_count=1)

                logger.info("Channel opened!")

            # Reset the connection attempts counter now there's a connection
            cxn_attempts = 0

            try:
                logger.info(f"Pulling message from {queue_name} " + \
                            f"queue on {exchange_name} exchange...")

                # Pull one message from the queue
                method_frame, header_frame, body = \
                    rabbitmq_channel.basic_get(queue_name, auto_ack=True)

                if method_frame:
                    logger.info("Message pulled from queue.")
                    process_message(body)
                else:
                    logger.info(f"No message pulled. Waiting {wait_time} " + \
                                "seconds before next pull...")
                    sleep(wait_time)

            except KeyboardInterrupt as error:
                rabbitmq_connection.close()
                logger.exception("RabbitMQ connection closed to due error: ",
                                exc_info=True)
                break
        except (pika.exceptions.ConnectionClosedByBroker,
                pika.exceptions.AMQPChannelError,
                pika.exceptions.AMQPConnectionError) as error:
            logger.exception(f"Connection closed by Broker to due error: {error}",
                             exc_info=True)
            if cxn_attempts < max_reconnect_attempts:
                logger.info(f"Waiting {wait_time} seconds before trying to " + \
                            "reconnect...")
                sleep(wait_time)
                cxn_attempts += 1
                logger.info("Trying to reconnect...")
                continue
            else:
                logger.exception("Unable to reestablish RabbitMQ connection " + \
                                 f"after {max_reconnect_attempts} attempts. " + \
                                 "Exiting...")
                break


if __name__ == "__main__":
    global listener_version
    global logger
    global schema

    # Initiate logger!
    logger = setup_log()

    # Load JSONSchema
    schema = load_schema()

    # Load listener version
    listener_version = get_listener_version()

    main()
