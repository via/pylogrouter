import pysyslog
import asyncio
import aiohttp

__all__ = ['RouterNode', 'SyslogSource', 'MemoryPipe', 'PrinterSink',
           'HTTPSink', 'HTTPSource']

class RouterNode():

    def __init__(self):
        self.consumers = []
        self.stats = { "eventsReceived": 0,
                       "eventsDelivered": 0,
                       "failedDeliveres": 0,
                       "failedReceives": 0 }
                            
 
    def addConsumer(self, consumer):
        self.consumers.append(consumer)

    def deliver(self, event):
        for consumer in self.consumers:
            if consumer.consume(event):
                self.stats['eventsDelivered'] += 1
            else:
                self.stats['failedDeliveries'] += 1

    def consume(event):
        self.deliver(event)

    def statistics(self):
        return self.stats

class SyslogWrapper(pysyslog.SyslogProtocol):
    maxbuffersize = 1024 * 1024 * 1024
    def __init__(self, source):
        self.source = source

    def handle_event(self, event):
        self.source.handle_event(event)

    def decode_error(self, str):
        self.source.decode_error(str)

    def overflow(self):
        self.source.overflow()

class SyslogSource(pysyslog.SyslogProtocol, RouterNode):
    maxbuffersize = 1024 * 1024 * 1024
    def __init__(self, address, tcp=False):
        RouterNode.__init__(self)
        loop = asyncio.get_event_loop()
        self.coro = loop.create_datagram_endpoint(lambda: SyslogWrapper(self), address)
        loop.run_until_complete(self.coro)

    def handle_event(self, event):
        RouterNode.deliver(self, event) 

    def decode_error(self, str):
        pass

    def overflow(self):
        pass 

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
            self.stats['failedReceives'] += 1
            return False
        self.stats['eventsReceived'] += 1
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
    def consume(self, event):
        print(event)
 
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
