# from .rabbitmq.rabbitmq import RabbitMQ
# import unittest
#
#
# class TestRabbitMQ(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls) -> None:
#         cls.rabbitmq = RabbitMQ(username="ragham",
#                                password="ragham147852",
#                                host="localhost",
#                                virtual_host="defhost",
#                                prefetch_count=1,
#                                queue_name='log',
#                                is_durable=True,
#                                callback_fuction=None)
#
#     def test_publish(self):
#         self.rabbitmq.perform_publishing(message='yes')
#         self.rabbitmq.close_connection()
