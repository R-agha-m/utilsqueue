from pika import PlainCredentials, ConnectionParameters, BlockingConnection, BasicProperties
from utils_common.manage_exceptions_decorator import manage_exceptions_decorator
try:
    from .stg import STG, report
except ImportError:
    from stg import STG, report


class RabbitMQ:
    def __init__(self, **kwargs):
        self.username = STG.BROKER.get("username") or STG.BROKER["USERNAME"]
        self.password = STG.BROKER.get("password") or STG.BROKER["PASSWORD"]
        self.host = STG.BROKER.get("host") or STG.BROKER["HOST"]
        self.port = STG.BROKER.get("port") or STG.BROKER["PORT"]
        self.vhost = STG.BROKER.get("vhost") or STG.BROKER["VHOST"]

        self.prefetch_count = STG.BROKER.get("prefetch_count") or STG.BROKER.get("PREFETCH_COUNT", 1)
        self.is_durable = STG.BROKER.get("is_durable") or STG.BROKER.get("IS_DURABLE", True)
        self.heartbeat = STG.BROKER.get("heartbeat") or STG.BROKER.get("HEARTBEAT", 1 * 60 * 60)

        self.queue = STG.BROKER.get("queue") or STG.BROKER["QUEUE"]

        self.channel = None
        self.credentials = None
        self.parameters = None
        self.basic_properties = None
        self.connection = None

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
                                               virtual_host=self.vhost,
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
                                   durable=self.is_durable)

    def _create_basic_properties(self):
        self.basic_properties = BasicProperties(delivery_mode=2, )

    def close_connection(self):
        self.connection.close()

    @manage_exceptions_decorator()
    def __del__(self):
        self.close_connection()
