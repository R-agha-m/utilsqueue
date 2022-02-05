from traceback import format_exc
from time import sleep

try:
    from .rabbitmq import RabbitMQ
    from .stg import STG, report
except ImportError:
    from rabbitmq import RabbitMQ
    from stg import STG, report


class Publisher(RabbitMQ):
    def __init__(self, **kwargs):
        if not kwargs:
            kwargs.update(STG.BROKER_DETAILS)

        new_kwargs = kwargs.copy()
        new_kwargs["queue"] = new_kwargs["publish_queue"]

        super().__init__(**new_kwargs)

    def perform_publishing_in_loop(self, message):
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

    def _basic_publish(self, message: str):
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=self.queue,
                                   body=message,
                                   properties=self.basic_properties)

        report.info("{} published to {}".format(message, self.queue))
