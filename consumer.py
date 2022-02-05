from traceback import format_exc
from time import sleep
from utils_common.exit_ import exit_

try:
    from .rabbitmq import RabbitMQ
    from .stg import STG, report
except ImportError:
    from rabbitmq import RabbitMQ
    from stg import STG, report


class Consumer(RabbitMQ):
    def __init__(self, **kwargs):
        if not kwargs:
            kwargs.update(STG.BROKER_DETAILS)

        new_kwargs = kwargs.copy()
        new_kwargs["queue"] = new_kwargs["consume_queue"]

        super().__init__(**new_kwargs)

        self.callback_function = lambda **fkwargs: STG.BROKER["callback_function"](**fkwargs,
                                                                                   rabbitmq_obj=self)

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

    def _basic_consume(self):
        self.channel.basic_consume(queue=self.queue,
                                   on_message_callback=self.callback_function)  # , auto_ack=True

    def _start_consuming(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            report.info(format_exc())
            exit_()
