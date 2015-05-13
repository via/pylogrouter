import pysyslog
import asyncio
import aiohttp

__all__ = ['RouterNode', 'SyslogSource', 'MemoryPipe', 'PrinterSink',
           'HTTPSink', 'HTTPSource']

class RouterNode():

    def __init__(self):
        self.consumers = []
        self.stats = { "eventsConsumed": 0,
                       "eventsDelivered": 0,
                       "failedConsumed": 0,
                       "failedDelivered": 0 }
                            
 
    def add_consumer(self, consumer):
        self.consumers.append(consumer)

    def deliver(self, event):
        for consumer in self.consumers:
            if consumer.consume(event):
                self.stats['eventsDelivered'] += 1
            else:
                self.stats['failedDelivered'] += 1

    def consume(event):
        self.deliver(event)
        self.stats['eventsConsumed'] += 1

    def statistics(self):
        return self.stats

class SyslogWrapper(pysyslog.SyslogProtocol):
    maxbuffersize = 1024 * 1024 * 1024
    def __init__(self, source):
        pysyslog.SyslogProtocol.__init__(self)
        self.source = source

    def handle_event(self, event):
        self.source.handle_event(event)

    def decode_error(self, str):
        self.source.decode_error(str)

    def overflow(self):
        self.source.overflow()

class SyslogSource(pysyslog.SyslogProtocol, RouterNode):
    def __init__(self, address, tcp=False):
        RouterNode.__init__(self)
        loop = asyncio.get_event_loop()
        if tcp:
            self.coro = loop.create_server(lambda: SyslogWrapper(self), address[0], address[1])
        else:
            self.coro = loop.create_datagram_endpoint(lambda: SyslogWrapper(self), address)
        loop.run_until_complete(self.coro)

    def handle_event(self, event):
        self.stats['eventsConsumed'] += 1
        RouterNode.deliver(self, event) 

    def decode_error(self, str):
        self.stats['failedConsumed'] += 1

    def overflow(self):
        self.stats['failedConsumed'] += 1

class MemoryPipe(RouterNode):
    def __init__(self, size):
        RouterNode.__init__(self)
        self.pipe = asyncio.Queue(size) 
        self.stats.update({"maxCapacity": size})
        self.dequeuer = asyncio.async(self.dequeue())

    def consume(self, event):
        try:
            self.pipe.put_nowait(event)
        except asyncio.QueueFull:
            self.stats['failedConsumed'] += 1
            return False
        self.stats['eventsConsumed'] += 1
        return True

    @asyncio.coroutine
    def dequeue(self):
        while True:
            event = yield from self.pipe.get()
            RouterNode.deliver(self, event) 

    def statistics(self):
        stats = self.stats
        stats['capacity'] = self.pipe.qsize()
        return stats


class PrinterSink(RouterNode):
    def __init__(self):
        RouterNode.__init__(self)
        self.stats = {"eventsConsumed": 0} 

    def consume(self, event):
        self.stats['eventsConsumed'] += 1
    #    print(event)
        return True
 
class HTTPSink(RouterNode):
    def __init__(self, uri, timeout):
        self.session = aiohttp.ClientSession()
        self.uri = uri
        self.timeout = timeout

    def consume(self, event):
        self.session.request('POST', uri, data=event)
        self.stats['eventsReceived'] += 1
        self.stats['eventsDelivered'] += 1

class HTTPSource(RouterNode):
    pass
