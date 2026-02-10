import os
import pika

class mqProducer():
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        self.m_routing_key = routing_key
        self.m_exchange_name = exchange_name

        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.m_connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.m_channel = self.m_connection.channel()

        # Create the exchange if not already present (topic exchange)
        self.m_channel.exchange_declare(
            exchange=self.m_exchange_name,
            exchange_type="topic",
        )

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.m_channel.basic_publish(
            exchange=self.m_exchange_name,
            routing_key=self.m_routing_key,
            body=message,
        )

        # Close Channel
        self.m_channel.close()

        # Close Connection
        self.m_connection.close()


