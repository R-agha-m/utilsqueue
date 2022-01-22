from pika import PlainCredentials, ConnectionParameters, BlockingConnection, BasicProperties
from traceback import format_exc
from time import sleep
from types import FunctionType
from ..apply_decorators_on_cls_methods_decorators import apply_decorators_on_cls_methods_decorators
from ..loggingUtils.logger_decorator import logger_decorator
import stg


# @apply_decorators_on_cls_methods_decorators((logger_decorator, (), {}))
class RabbitMQ:
    def __init__(self,
                 username: str = None,
                 password: str = None,
                 host: str = None,
                 port: int = None,
                 virtual_host: str = None,
                 prefetch_count: int = None,
                 queue_name: str = '',
                 is_durable: bool = None,
                 callback_function: FunctionType = None,
                 heartbeat: int = None):

        self.username = username or stg.BROKER_DETAILS.get('username')
        self.password = password or stg.BROKER_DETAILS['password']
        self.host = host or f"{stg.BROKER_DETAILS['host']}"
        self.port = port or int(stg.BROKER_DETAILS['port'])
        self.virtual_host = virtual_host or stg.BROKER_DETAILS['vhost']
        self.prefetch_count = prefetch_count or stg.PREFETCH_COUNT
        self.queue = queue_name
        self.durable = is_durable if is_durable is not None else stg.IS_DURABLE
        self.callback_function = \
            lambda ch, method, properties, body: callback_function(ch, method, properties, body, self)
        self.heartbeat = heartbeat or stg.HEARTBEAT

        self.channel = None
        self.credentials = None
        self.parameters = None
        self.basic_properties = None
        self.connection = None
        self.configuration = None

    def perform_publishing_in_loop(self, message='no message'):
        while True:
            try:
                return self.perform_publishing(message)
            except Exception:
                stg.report.info(format_exc())
                sleep(stg.SLEEP_TIME_AFTER_PUBLISHING_ERROR)

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
                stg.report.info(format_exc())
                sleep(stg.SLEEP_TIME_AFTER_CONSUMING_ERROR)

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

        stg.report.info("{} published to {}".format(message, self.queue))

    def _basic_consume(self):
        self.channel.basic_consume(queue=self.queue,
                                   on_message_callback=self.callback_function)  # , auto_ack=True

    def _start_consuming(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('Interrupted')
            exit_()

    def close_connection(self):
        self.connection.close()
