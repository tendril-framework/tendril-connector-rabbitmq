# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205


"""
# based on:
#  - Pika Example from
#    https://github.com/pika/pika/blob/master/examples/twisted_service.py
#  - txamqp-helpers by Dan Siemon <dan@coverfire.com> (March 2010)
#    http://git.coverfire.com/?p=txamqp-twistd.git;a=tree
#  - Post by Brian Chandler
#    https://groups.google.com/forum/#!topic/pika-python/o_deVmGondk
#  - Pika Documentation
#    https://pika.readthedocs.io/en/latest/examples/twisted_example.html
"""


from twisted.internet import protocol
from twisted.application import internet
from twisted.application import service
from twisted.internet.defer import inlineCallbacks
from twisted.internet import ssl, defer

from pika import spec
from pika.adapters import twisted_connection
from pika.exchange_type import ExchangeType
from tendril.tx.utils.logger import TwistedLoggerMixin

PREFETCH_COUNT = 20


class PikaService(service.MultiService, TwistedLoggerMixin):
    def __init__(self, parameter, postfix=None):
        self._postfix = postfix
        TwistedLoggerMixin.__init__(self)
        service.MultiService.__init__(self)
        self.parameters = parameter

    @property
    def name(self):
        if self._postfix:
            return 'amqp091:{}'.format(self._postfix)
        return 'amqp091'

    def startService(self):
        self.connect()
        service.MultiService.startService(self)

    def getFactory(self):
        return self.services[0].factory

    def connect(self):
        f = PikaFactory(self.parameters)
        if self.parameters.ssl_options:
            s = ssl.ClientContextFactory()
            serv = internet.SSLClient(  # pylint: disable=E1101
                host=self.parameters.host,
                port=self.parameters.port,
                factory=f,
                contextFactory=s)
        else:
            serv = internet.TCPClient(  # pylint: disable=E1101
                host=self.parameters.host,
                port=self.parameters.port,
                factory=f)
        serv.factory = f
        f.service = serv  # pylint: disable=W0201
        name = '%s%s:%d' % ('ssl:' if self.parameters.ssl_options else '',
                            self.parameters.host, self.parameters.port)
        serv.__repr__ = lambda: '<AMQP Connection to %s>' % name
        serv.setName(name)
        serv.setServiceParent(self)


class PikaProtocol(twisted_connection.TwistedProtocolConnection, TwistedLoggerMixin):
    connected = False
    name = 'AMQP:Protocol'

    def __init__(self, factory, parameters):
        self._channel = None
        TwistedLoggerMixin.__init__(self)
        super(PikaProtocol, self).__init__(parameters)
        self.factory = factory

    @inlineCallbacks
    def connectionReady(self):
        self._channel = yield self.channel()
        yield self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        self.connected = True
        yield self._channel.confirm_delivery()
        for (
                exchange,
                routing_key,
                callback,
                queue,
                exclusive,
                durable
        ) in self.factory.read_list:
            yield self.setup_read(exchange, routing_key, callback,
                                  queue, exclusive, durable)
        self.send()

    @inlineCallbacks
    def read(self, exchange, routing_key, callback, queue, exclusive, durable):
        """Add an exchange to the list of exchanges to read from."""
        if self.connected:
            yield self.setup_read(exchange, routing_key, callback, queue, exclusive, durable)

    @inlineCallbacks
    def setup_read(self, exchange, routing_key, callback, queue, exclusive, durable):
        """This function does the work to read from an exchange."""
        if exchange:
            yield self._channel.exchange_declare(
                exchange=exchange,
                exchange_type=ExchangeType.topic,
                durable=True,
                auto_delete=False)

        if not queue:
            queue = routing_key
            durable = True
            exclusive = False

        yield self._channel.queue_declare(queue=queue,
                                          exclusive=exclusive,
                                          durable=durable)

        if exchange:
            yield self._channel.queue_bind(queue=queue, exchange=exchange)
            yield self._channel.queue_bind(
                queue=queue, exchange=exchange, routing_key=routing_key)

        (
            _queue,
            _consumer_tag,
        ) = yield self._channel.basic_consume(
            queue=queue, auto_ack=False)
        d = _queue.get()
        d.addCallback(self._read_item, _queue, callback)
        d.addErrback(self._read_item_err)

    def _read_item(self, item, queue, callback):
        """Callback function which is called when an item is read."""
        d = queue.get()
        d.addCallback(self._read_item, queue, callback)
        d.addErrback(self._read_item_err)
        (
            channel,
            deliver,
            _props,
            message,
        ) = item

        # log.message(
        #     '%s (%s): %s' % (deliver.exchange, deliver.routing_key, repr(message)),
        #     system='Pika:<=')
        d = defer.maybeDeferred(callback, item)
        d.addCallbacks(lambda _: channel.basic_ack(deliver.delivery_tag),
                       lambda _: channel.basic_nack(deliver.delivery_tag))

    @staticmethod
    def _read_item_err(error):
        print(error)

    def send(self):
        """If connected, send all waiting messages."""
        if self.connected:
            while self.factory.queued_messages:
                (
                    exchange,
                    routing_key,
                    message,
                    properties,
                ) = self.factory.queued_messages.pop(0)
                self.send_message(exchange, routing_key, message, properties)

    @inlineCallbacks
    def send_message(self, exchange, routing_key, message, properties=None):
        """Send a single message."""
        # log.message(
        #     '%s (%s): %s' % (exchange, routing_key, repr(message)),
        #     system='Pika:=>')
        yield self._channel.exchange_declare(
            exchange=exchange,
            exchange_type=ExchangeType.topic,
            durable=True,
            auto_delete=False)

        if properties is None:
            properties = {}
        properties.setdefault('delivery_mode', 2)
        _properties = spec.BasicProperties(**properties)

        try:
            yield self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
                properties=_properties)
        except Exception as error:  # pylint: disable=W0703
            self.log.error('Error while sending message: %s' % error, system=self.name)


class PikaFactory(protocol.ReconnectingClientFactory, TwistedLoggerMixin):
    name = 'AMQP:Factory'

    def __init__(self, parameters):
        TwistedLoggerMixin.__init__(self)
        super(PikaFactory, self).__init__()
        self.parameters = parameters
        self.client = None
        self.queued_messages = []
        self.read_list = []

    def startedConnecting(self, connector):
        self.log.info('Started to connect.', system=self.name)

    def buildProtocol(self, addr):
        self.resetDelay()
        self.log.info('Connected', system=self.name)
        self.client = PikaProtocol(self, self.parameters)
        return self.client

    def clientConnectionLost(self, connector, reason): # pylint: disable=W0221
        self.log.warn('Lost connection.  Reason: %s' % reason.value, system=self.name)
        protocol.ReconnectingClientFactory.clientConnectionLost(
            self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        self.log.error(
            'Connection failed. Reason: %s' % reason.value, system=self.name)
        protocol.ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason)

    def send_message(self, exchange, routing_key, message, properties=None):
        self.queued_messages.append((exchange, routing_key, message, properties))
        if self.client is not None:
            self.client.send()

    def read_messages(self, exchange, routing_key, callback, queue=None, exclusive=True, durable=False):
        """Configure an exchange to be read from."""
        self.read_list.append((exchange, routing_key, callback, queue, exclusive, durable))
        if self.client is not None:
            self.client.read(exchange, routing_key, callback, queue, exclusive, durable)

    def __repr__(self):
        return f"<{self.__class__.__name__}>"

MQService = PikaService
MQProtocol = PikaProtocol
MQFactory = PikaFactory
