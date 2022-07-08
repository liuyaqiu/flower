import logging
from kombu import Queue

from celery.events.receiver import EventReceiver
from kombu.exceptions import MessageStateError

logger = logging.getLogger(__name__)


class CustomEventReceiver(EventReceiver):
    # You can bind multiple queue rather than one queue for a event receiver.
    def __init__(self, channel, recoverable_errors=[], **kwargs):
        super().__init__(channel, **kwargs)

        # Event queue should be durable.
        del self.queue
        self.ack_queues = []
        self.no_ack_queues = []
        assert isinstance(recoverable_errors, (list, tuple))
        for error in recoverable_errors:
            assert issubclass(error, Exception)
        self.recoverable_errors = recoverable_errors

    def bind_queue(self, queue_name, routing_key,
                   auto_delete=True, durable=False,
                   queue_ttl=None, queue_expires=None, no_ack=True):
        # Event queue should be durable.
        queue = Queue(
            queue_name,
            exchange=self.exchange,
            routing_key=routing_key,
            auto_delete=auto_delete, durable=durable,
            message_ttl=queue_ttl,
            expires=queue_expires,
        )
        if no_ack:
            self.no_ack_queues.append(queue)
        else:
            self.ack_queues.append(queue)

    def get_consumers(self, Consumer, channel):
        ack_consumer = Consumer(
            queues=self.ack_queues,
            callbacks=[self._ack_receive],
            no_ack=False,
            accept=self.accept
        )
        no_ack_consumer = Consumer(
            queues=self.no_ack_queues,
            callbacks=[self._receive],
            no_ack=True,
            accept=self.accept
        )
        return [ack_consumer, no_ack_consumer]

    def _ack_receive(self, body, message, list=list, isinstance=isinstance):
        try:
            if isinstance(body, list):  # celery 4.0+: List of events
                process, from_message = self.process, self.event_from_message
                [process(*from_message(event)) for event in body]
            else:
                self.process(*self.event_from_message(body))
        except self.recoverable_errors as e:
            logger.error("Reject and requeue for message: {}.\nbecause of error: {}".format(body, e))
            logger.exception(e)
            # The task event should always be consumed successfully.
            reject_errors = (MessageStateError,)
            message.reject_log_error(
                requeue=True, logger=logging, errors=reject_errors)
        else:
            logger.debug('Process done, ack message: {}'.format(body))
            message.ack()
