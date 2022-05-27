import pika
import uuid
import json


class WikiRpcClient:

    def __init__(self):
        self.corr_id = None
        self.response = None
        self.callback_queue = None
        self.channel = None
        self.connection = None

    def refresh_state(self):
        if self.connection:
            self.connection.close()
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1'))
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
        self.refresh_state()
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
        self.refresh_state()
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
