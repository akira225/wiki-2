import pika
import uuid
import json


class WikiRpcClient:

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1'))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='')
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def basic_check(self):
        if not self.connection or self.connection.is_closed:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='127.0.0.1'))
            self.channel = self.connection.channel()

            result = self.channel.queue_declare(queue='')
            self.callback_queue = result.method.queue

            self.channel.basic_consume(
                queue=self.callback_queue,
                on_message_callback=self.on_response,
                auto_ack=True)
        if not self.channel or self.channel.is_closed:
            self.channel = self.connection.channel()

            result = self.channel.queue_declare(queue='')
            self.callback_queue = result.method.queue

            self.channel.basic_consume(
                queue=self.callback_queue,
                on_message_callback=self.on_response,
                auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call_validate(self, article):
        self.basic_check()
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps({"action": "validate", "data": {"article": article}}))
        while self.response is None:
            self.connection.process_data_events()
        return json.loads(self.response)

    def call_path(self, A, B):
        self.basic_check()
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps({"action": "path", "data": {"A": A, "B": B}}))
        while self.response is None:
            self.connection.process_data_events()
        return json.loads(self.response)
