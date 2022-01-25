try:
    from .rabbitmq import RabbitMQ
    from .stg import STG
except ImportError:
    from rabbitmq import RabbitMQ
    from stg import STG


class Consumer:
    def __init__(self,
                 callback_func,
                 queue_name=STG.QUEUE_NAME):
        self.callback_func = callback_func
        self.queue_name = queue_name

        self.rabbitmq_consumer = None

    def perform(self):
        self._create_rabbitmq_instance()
        self._start_consuming()

    def _create_rabbitmq_instance(self):
        self.rabbitmq_consumer = RabbitMQ(queue_name=self.queue_name,
                                          callback_function=self.callback_func)

    def _start_consuming(self):
        self.rabbitmq_consumer.perform_consuming_in_loop()

#
# if __name__ == "__main__":
#     my_consumer = Consumer()
#     my_consumer.perform()
