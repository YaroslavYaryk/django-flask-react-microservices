import pika, json

params = pika.URLParameters('amqps://bchoukaa:nmy6nGxY3nZaJDKY0s2zEwzjMr4G36Pw@cow.rmq2.cloudamqp.com/bchoukaa')

connection = pika.BlockingConnection(params)

channel = connection.channel()


def publish(method, body):
    properties = pika.BasicProperties(method)
    channel.basic_publish(exchange='', routing_key='main', body=json.dumps(body), properties=properties)