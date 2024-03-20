#!/usr/bin/env python3
import json
import pika

config = { 'rabbitmq':{ 
                'host':'porpoise.rmq.cloudamqp.com',
                'port':'5672',
                'virtual-host':'cykdlvsx',
                'username':'cykdlvsx',
                'password':'CmqxNuE3rxCMckjXbPLiIQe185oyzNhR',
                'consumer-exchange-name':'exch_1',
                'consumer-queue-name':'manifest_validation_status',
                'consumer-routing-key':'manifest_validation_status'
        }   
}


# RabbitMQ credentials for publisher and consumer
rabbitmq_credentials = pika.PlainCredentials(
    username=config['rabbitmq']['username'],
    password=config['rabbitmq']['password']
)


def process_message(body):
    """
    Process the message received from RabbitMQ.
    """
    print("In process_message().")

    decoded_str = body.decode("utf-8")
    data = json.loads(decoded_str)
    formatted_msg = json.dumps(data, indent=2)
    print(f"Message received from RabbitMQ:\n{formatted_msg}.")
    print("Done.")


def get_rabbitmq_channel(rabbitmq_connection, exchange_name, queue_name,
                         routing_key):
    """
    Returns a RabbitMQ pika.channel.Channel
    """
    print("In get_rabbitmq_channel().")

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


def main():
    """
    Set up the RabbitMQ consumer.
    """
    print("Setting up RabbmitMQ consumer.")

    # Get queue and connection retry configuration
    exchange_name = config['rabbitmq']['consumer-exchange-name']
    queue_name = config['rabbitmq']['consumer-queue-name']
    routing_key = config['rabbitmq']['consumer-routing-key']

    rabbitmq_connection = None
    rabbitmq_channel = None

    try:
        if not rabbitmq_connection or rabbitmq_connection.is_closed:
            host = config['rabbitmq']['host']
            print(f"Connecting to RabbitMQ at {host}")

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
            print("Connected to RabbitMQ!")

        if not rabbitmq_channel or rabbitmq_channel.is_closed:
            # Get a connection channel.
            print("Opening channel...")
            rabbitmq_channel = get_rabbitmq_channel(
                rabbitmq_connection,
                exchange_name,
                queue_name,
                routing_key
            )

            # Set max number of messages a consumer can pull at one time.
            rabbitmq_channel.basic_qos(prefetch_count=1)

            print("Channel opened!")

        try:
            print(f"Pulling message from {queue_name} " + \
                        f"queue on {exchange_name} exchange...")

            # Pull one message from the queue
            method_frame, header_frame, body = \
                rabbitmq_channel.basic_get(queue_name, auto_ack=True)

            if method_frame:
                print("Message pulled from queue.")
                process_message(body)
            else:
                print(f"No message pulled.")

        except KeyboardInterrupt as error:
            rabbitmq_connection.close()
            print("RabbitMQ connection closed to due error: ")

    except (pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.AMQPConnectionError) as error:
        print(f"Connection closed by Broker to due error: {error}")


if __name__ == "__main__":
    main()
