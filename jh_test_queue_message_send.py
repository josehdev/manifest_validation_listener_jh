import json
import pika
import traceback
import datetime  


payload = {
    'submission_id': 'z8cawiz',
    'manifest_path': 'https://owigseusnemodev.blob.core.windows.net/nemo-manifest-submissions/manifest-2024-01-30-10_21_21-z8cawiz.tsv',
    'project_name': 'nemo',
    'original_filename': 'test_azure_manifest.txt',
    'submitter': {
        'username': 'jherrera',
        'first': 'Jose',
        'last': 'Herrera',
        'email': 'josehsc.h@gmail.com'
    },
    'program': 'bican',
    'result': True,
    'dryrun': False
}

rabbitmq_config = {
        'publisher_queue_name':'manifest_validation_status',
        'publisher_routing_key':'manifest_validation_status',
        'rabbitmq_host':'porpoise.rmq.cloudamqp.com',
        'rabbitmq_port':'5672',
        'rabbitmq_virtual_host':'cykdlvsx',
        'rabbitmq_username':'cykdlvsx',
        'rabbitmq_password':'CmqxNuE3rxCMckjXbPLiIQe185oyzNhR',
        'publisher_exchange_name':'exch_1'
}

def get_rabbitmq_channel(rabbitmq_connection, exchange_name, queue_name, routing_key):
    """
    Returns a RabbitMQ pika.channel.Channel
    """
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


def notify_nemo(payload):
    """
    Pushes the JSON payload to the RabbitMQ queue.
    This queues the NeMO submission for the next step in ingest.
    """
    payload_str = json.dumps(payload)

    try:
        rabbitmq_host = rabbitmq_config['rabbitmq_host']
        rabbitmq_port =  int(rabbitmq_config['rabbitmq_port'])
        rabbitmq_virtual_host = rabbitmq_config['rabbitmq_virtual_host']
        rabbitmq_username = rabbitmq_config['rabbitmq_username']
        rabbitmq_password = rabbitmq_config['rabbitmq_password']
        publisher_exchange_name = rabbitmq_config['publisher_exchange_name']
        publisher_queue_name = rabbitmq_config['publisher_queue_name']
        publisher_routing_key = rabbitmq_config['publisher_routing_key']

        # RabbitMQ credentials for publisher
        rabbitmq_credentials = pika.PlainCredentials(
            username=rabbitmq_username,
            password=rabbitmq_password
        )

        # Parameters for RabbitMQ producer
        cxn_parameters = pika.ConnectionParameters(
            host=rabbitmq_host,
            port=rabbitmq_port,
            virtual_host=rabbitmq_virtual_host,
            credentials=rabbitmq_credentials
        )

        # Get connection to RabbitMQ instance on GCP
        rabbitmq_connection = pika.BlockingConnection(
            parameters=cxn_parameters
        )

        # Get a connection channel.
        channel = get_rabbitmq_channel(
            rabbitmq_connection,
            publisher_exchange_name,
            publisher_queue_name,
            publisher_routing_key
        )

        # Publish message to the next step's queue.
        channel.basic_publish(
            exchange=publisher_exchange_name,
            routing_key=publisher_routing_key,
            body=payload_str,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )

        rabbitmq_connection.close()
        print(f"RabbitMQ message published to {publisher_queue_name} queue.")
    except Exception:
        tb_message = traceback.format_exc()
        print(f"An error occurred while publishing message to RabbitMQ: {tb_message}")


notify_nemo(payload)

print('Done!')