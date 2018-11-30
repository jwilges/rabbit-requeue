#!/usr/bin/env python
import os

class State(object):
    def __init__(self, configuration_file_name: os.PathLike):
        import pika
        import yaml

        with open(configuration_file_name, 'r') as configuration_file:
            configuration = yaml.safe_load(configuration_file)

        source = configuration['source']
        source_credentials = pika.PlainCredentials(source['username'], source['password'])
        self.source_parameters = pika.ConnectionParameters(
            source['host'],
            source['port'],
            source['virtual_host'],
            source_credentials)
        self.source_queue = source['queue']

        receive_rate = source['receive_rate']
        if receive_rate == 0:
            self.publish_delay = 0.001
        else:
            self.publish_delay = 1 / receive_rate

        sink = configuration['sink']
        sink_credentials = pika.PlainCredentials(sink['username'], sink['password'])
        self.sink_parameters = pika.ConnectionParameters(
            sink['host'],
            sink['port'],
            sink['virtual_host'],
            sink_credentials)
        self.sink_queue = sink['queue']


    def __enter__(self):
        import pika
        self.source_connection = pika.BlockingConnection(self.source_parameters)
        self.source_channel = self.source_connection.channel()
        self.sink_connection = pika.BlockingConnection(self.sink_parameters)
        self.sink_channel = self.sink_connection.channel()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.source_channel.cancel()
        self.source_connection.close()
        self.sink_channel.cancel()
        self.sink_connection.close()
        return True


if __name__ == '__main__':
    import argparse
    import sys
    import time

    default_configuration_file_name = 'configuration.yml'

    argument_parser = argparse.ArgumentParser(os.path.basename(sys.argv[0]))
    argument_parser.add_argument('-c', '--config',
        type=argparse.FileType('r'), default=default_configuration_file_name,
        help=f'source/sink queue configuration file (default: "{default_configuration_file_name}")')

    arguments = argument_parser.parse_args()
    configuration_file_name = arguments.config.name
    arguments.config.close()

    with State(configuration_file_name) as state:
        state.source_channel.queue_declare(queue=state.source_queue, durable=True)
        state.sink_channel.queue_declare(queue=state.sink_queue)

        print('Consuming messages...')
        try:
            for method_frame, properties, body in state.source_channel.consume(queue=state.source_queue):
                time.sleep(state.publish_delay)
                print(f'Publishing: {body}')
                state.sink_channel.basic_publish(exchange='', routing_key=state.sink_queue, body=body)
                # ch.basic_ack(delivery_tag=method.delivery_tag)
        except KeyboardInterrupt:
            print('Exiting due to keyboard interrupt...')

    print('Finished.')
