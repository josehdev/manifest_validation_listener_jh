#!/usr/bin/env python3

import argparse
import configparser
import json
from datetime import datetime
import os
import requests
import sys

sys.path.append("/local/projects-t3/NEMO/bin/ingest_scripts/lib")
sys.path.append("/local/projects-t3/NEMO/pylibs")
import pika


gcp_project = "nemo"

# Read the config file
conf_loc = os.path.join(os.path.dirname(__file__), '..', 'conf.ini')
if not os.path.isfile(conf_loc):
    sys.exit("Config file could not be found at {}".format(conf_loc))
config = configparser.ConfigParser()
config.read(conf_loc)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit a NeMO submission message to the manifest validation queue ."
    )

    parser.add_argument(
        "--username", "-u",
        help="The username to use for the test submission. Should be a real NeMO username.",
        type=str,
        required=True
    )

    parser.add_argument(
        "--program",
        help="The program (biccn, bican, scorch, etc) to use for the submission.",
        type=str,
        required=True
    )

    parser.add_argument(
        "--dryrun",
        help="Whether the test should indicate a dryrun or not.",
        action="store_true"
    )

    args = parser.parse_args()

    return args


def get_rabbitmq_channel(rabbitmq_connection, exchange_name, queue_name, routing_key):
    """
    Returns a RabbitMQ pika.channel.Channel
    """

    channel = rabbitmq_connection.channel()

    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type="direct",
        durable=True
    )

    # Set queue to only have 1 active consumer & set the max priority for
    # messages. To enforce priority, messages must manually ack'ed in
    # consumer's callback function and prefetch count should be reduced.
    # Priority Docs: https://www.rabbitmq.com/priority.html
    channel.queue_declare(queue=queue_name,
        durable=True,
        arguments={
            "x-single-active-consumer": True
        }
    )

    # Establish relationship between exchange and queue.
    channel.queue_bind(
        exchange=exchange_name,
        queue=queue_name,
        routing_key=routing_key
    )

    # Set max number of messages a consumer can pull at one time.
    channel.basic_qos(prefetch_count=1)

    return channel


def get_rabbitmq_connection(config):
    rabbitmq_credentials = pika.PlainCredentials(
        username=config['rabbitmq']['username'],
        password=config['rabbitmq']['password']
    )

    cxn_parameters = pika.ConnectionParameters(
        host=config['rabbitmq']['host'],
        port=config['rabbitmq']['port'],
        virtual_host=config['rabbitmq']['virtual-host'],
        credentials=rabbitmq_credentials
    )

    # Get connection to RabbitMQ
    connection = pika.BlockingConnection(parameters=cxn_parameters)

    return connection


def open_rabbitmq_channel(rabbitmq_connection, exchange_name, queue_name, routing_key):
    # Get a connection channel.
    channel = get_rabbitmq_channel(
        rabbitmq_connection,
        exchange_name,
        queue_name,
        routing_key
    )

    return channel


def get_submitter_info(username):
    """
    Looks up metadata about the NeMO user such that a personalized email can be
    sent and so that further steps along the ingest process can also have access
    to this metadata for similar purposes.
    """
    endpoint = f"http://nemo-internal.igs.umaryland.edu/v1/users/{username}"

    response = requests.get(endpoint)

    if response.status_code == 200:
        metadata = json.loads(response.text)
        return metadata
    else:
        raise Exception("Unable to lookup submitter information.")


def get_timestamp():
    """
    Returns a properly formatted timestamp string.
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return now


def publish_to_queue(exchange_name, queue_name, routing_key,
    payload, message_priority):
    """
    Pushes the JSON payload to the RabbitMQ queue.
    This queues the NeMO submission for the next step in ingest.

    Returns None if message was successfully published, otherwise returns
    error message string after a number of failed publish attempts.
    """

    payload_str = json.dumps(payload)

    err_msg = None

    try:
        rabbitmq_connection = get_rabbitmq_connection(config)

        channel = open_rabbitmq_channel(
            rabbitmq_connection,
            exchange_name,
            queue_name,
            routing_key
        )

        # Publish message to the next step's queue.
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=payload_str,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                priority=message_priority
            )
        )

        rabbitmq_connection.close()
    except Exception as err:
        sys.stderr.write("Unable to publish message to RabbitMQ: {err}\n")
        raise err


def publish_manifest_validation_submission(username, program, dryrun):
    """
    Builds a submission message and submits to RabbitMQ queue.
    """

    # Fetch submitter metadata from NeMO users db
    print("Fetching submitter information...")
    submitter = get_submitter_info(username)

    payload = {
        "submission_id": "xxxxxxx",
        "program": program,
        "submitter": submitter,
        "result": False,
        "complete_errors": False,
        "errors": [
            "This is test error message 1.",
            "This is test error message 2."
            "This is test error message 3."
            "This is test error message 4."
            "This is test error message 5."
        ],
        "original_filename": "my_test_manifest.tsv",
        "gcp_manifest_path": f"gs://{gcp_project}-manifest-submissions-devel/manifest-1970-01-01-00:00:01-xxxxxxx.tsv",
        "gcp_error_path": f"gs://{ gcp_project }-manifest-submission-errors-devel/manifest-1970-01-01-00:00:01-xxxxxxx.tsv.errors",
        "gcp_project_name": gcp_project
    }

    if dryrun:
        payload['dryrun'] = dryrun

    err_msg = publish_to_queue(
        exchange_name="nemo-ingest-devel",
        queue_name="manifest-validation-status-devel",
        routing_key="manifest-validation-status-devel",
        payload=payload,
        message_priority=1
    )

    if err_msg:
        raise Exception(err_msg)

    print(f"Published message with submission id xxxxxxx.")


def main():
    args = parse_args()

    program = args.program
    username = args.username
    dryrun = args.dryrun

    publish_manifest_validation_submission(username, program, dryrun)


main()
