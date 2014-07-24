from queues import queues_dict
from queues import task_exchange
from kombu.mixins import ConsumerMixin
from kombu.common import maybe_declare
from kombu.common import send_reply
from kombu.pools import producers


class S(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues_dict['first_VM'], accept=['json'],
                callbacks=[self.on_message])]

    def on_message(self, body, message):
        print ("RECEIVED MSG FROM CLIENT - body: %r" % (body,))
        message.ack()
        self.set_message(message=message)
        return

    def set_message(self, message):
        json_arr = []
        qcow_info = {}
        qcow_info["filename"] = "/var/db/images/vm1/disk"
        qcow_info["size"] = 1480
        json_arr.append(qcow_info)

        with producers[connection].acquire(block=True) as producer:
            maybe_declare(task_exchange, producer.channel)
            send_reply(task_exchange, message, json_arr, producer=producer)
        return

if __name__ == "__main__":
    from kombu import BrokerConnection
    from kombu.utils.debug import setup_logging

    setup_logging(loglevel="DEBUG")

    with BrokerConnection("amqp://guest:guest@localhost:5672//") as connection:
        try:
            S(connection).run()
        except KeyboardInterrupt:
            print("bye bye")
