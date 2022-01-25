from pika import PlainCredentials, ConnectionParameters, BlockingConnection, BasicProperties
from traceback import format_exc
from time import sleep
from types import FunctionType
from utils_common.exit_ import exit_

try:
    from .stg import STG, report
except ImportError:
    from stg import STG, report


class RabbitMQ:
    def __init__(self,
                 username: str = STG.BROKER["username"],
                 password: str = STG.BROKER["password"],
                 host: str = STG.BROKER["host"],
                 port: int = STG.BROKER["port"],
                 virtual_host: str = STG.BROKER["virtual_host"],
                 prefetch_count: int = STG.BROKER["prefetch_count"],
                 queue_name: str = STG.BROKER["publish_queue_name"],
                 is_durable: bool = STG.BROKER["is_durable"],
                 callback_function: FunctionType = None,
                 heartbeat: int = STG.BROKER["heartbeat"]):

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.prefetch_count = prefetch_count
        self.queue = queue_name
        self.durable = is_durable
        self.callback_function = \
            lambda ch, method, properties, body: callback_function(ch, method, properties, body, self)
        self.heartbeat = heartbeat

        self.channel = None
        self.credentials = None
        self.parameters = None
        self.basic_properties = None
        self.connection = None
        self.configuration = None

    def perform_publishing_in_loop(self,
                                   message):
        while True:
            try:
                return self.perform_publishing(message)
            except Exception:
                report.info(format_exc())
                sleep(STG.TIME_OUT)

    def perform_publishing(self, message):
        self._initialize()
        self._create_basic_properties()
        self._declare_exchange()
        self._basic_publish(message)

    def perform_consuming_in_loop(self):
        while True:
            try:
                self.perform_consuming()
            except Exception:
                report.info(format_exc())
                sleep(STG.TIME_OUT)

    def perform_consuming(self):
        self._initialize()
        self._basic_consume()
        self._start_consuming()

    def _initialize(self):
        self._create_credentials_parameters()
        self._create_connection_parameters()
        self._create_blocking_connection()
        self._create_channel()
        self._set_channel_properties()
        self._declare_queue()

    def _create_credentials_parameters(self):
        self.credentials = PlainCredentials(username=self.username,
                                            password=self.password)

    def _create_connection_parameters(self):
        self.parameters = ConnectionParameters(host=self.host,
                                               port=self.port,
                                               virtual_host=self.virtual_host,
                                               credentials=self.credentials,
                                               heartbeat=self.heartbeat)

    def _create_blocking_connection(self):
        self.connection = BlockingConnection(self.parameters)

    def _create_channel(self):
        self.channel = self.connection.channel()

    def _set_channel_properties(self):
        self.channel.basic_qos(prefetch_count=self.prefetch_count)

    def _declare_exchange(self):
        self.exchange = ''
        # channel.exchange_declare(exchange='acex',
        #                          exchange_type='fanout')

    def _declare_queue(self):
        self.channel.queue_declare(queue=self.queue,
                                   durable=self.durable)

    def _create_basic_properties(self):
        self.basic_properties = BasicProperties(delivery_mode=2, )

    def _basic_publish(self, message: str):
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=self.queue,
                                   body=message,
                                   properties=self.basic_properties)

        report.info("{} published to {}".format(message, self.queue))

    def _basic_consume(self):
        self.channel.basic_consume(queue=self.queue,
                                   on_message_callback=self.callback_function)  # , auto_ack=True

    def _start_consuming(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            report.info(format_exc())
            exit_()

    def close_connection(self):
        self.connection.close()
