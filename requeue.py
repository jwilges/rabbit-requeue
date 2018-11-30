import time
import pika
import yaml

with open('dev.yml', 'r') as configuration_file:
    config = yaml.safe_load(configuration_file)

credentials = pika.PlainCredentials(config['username'], config['password'])
parameters = pika.ConnectionParameters(config['host'],
                                       config['port'],
                                       config['virtual_host'],
                                       credentials)

publish_delay = 1 / config['receive_rate']
source_queue = config['source_queue']
sink_queue = config['sink_queue']

with pika.BlockingConnection(parameters) as connection:
    with connection.channel() as channel:
        channel.queue_declare(queue=source_queue, durable=True)
        channel.queue_declare(queue=sink_queue)

        print('Consuming messages...')
        try:
            for method_frame, properties, body in channel.consume(queue=source_queue):
                time.sleep(publish_delay)
                print(f'Publishing: {body}')
                channel.basic_publish(exchange='', routing_key=sink_queue, body=body)
                # ch.basic_ack(delivery_tag=method.delivery_tag)
        except KeyboardInterrupt:
            pass

        channel.cancel()
        print('Finished.')