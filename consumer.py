from .rabbitmq import RabbitMQ
from callback_function.callback_function import Callback
from ..apply_decorators_on_cls_methods_decorators import apply_decorators_on_cls_methods_decorators
from ..loggingUtils.logger_decorator import logger_decorator
import stg


# @apply_decorators_on_cls_methods_decorators((logger_decorator, (), {}))
class Consumer:
    def __init__(self,
                 callback_func,
                 queue_name=None):

        self.callback_func = callback_func
        self.queue_name = queue_name or stg.QUEUE_NAME

        self.rabbitmq_consumer = None

    def perform(self):
        self._create_rabbitmq_instance()
        self._start_consuming()

    def _create_rabbitmq_instance(self):
        self.rabbitmq_consumer = RabbitMQ(queue_name=self.queue_name,
                                          callback_function=self.callback_func)

    def _start_consuming(self):
        self.rabbitmq_consumer.perform_consuming_in_loop()


if __name__ == "__main__":
    my_consumer = Consumer()
    my_consumer.perform()
