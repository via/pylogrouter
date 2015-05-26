import pysyslog
import asyncio
import aiohttp
import json

__all__ = ['RouterNode', 'SyslogSource', 'MemoryPipe', 'PrinterSink',
           'HTTPSink', 'HTTPSource', 'StaticHeader', 'FlumeHTTPSink']

class RouterNode():

    def __init__(self):
        self.consumers = []
        self.stats = { "eventsConsumed": 0,
                       "eventsDelivered": 0,
                       "failedConsumed": 0,
                       "failedDelivered": 0 }
                            
 
    def add_consumer(self, consumer):
        self.consumers.append(consumer)

    @asyncio.coroutine
    def deliver(self, event):
        for consumer in self.consumers:
            success = yield from consumer.consume(event)
            if success:
                self.stats['eventsDelivered'] += 1
            else:
                self.stats['failedDelivered'] += 1

    @asyncio.coroutine
    def consume(event):
        yield from self.deliver(event)
        self.stats['eventsConsumed'] += 1
        return True

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
    def __init__(self, address, port, tcp=False):
        port=int(port)
        RouterNode.__init__(self)
        self.loop = asyncio.get_event_loop()
        if tcp:
            self.coro = self.loop.create_server(lambda: SyslogWrapper(self), address, port)
        else:
            self.coro = self.loop.create_datagram_endpoint(lambda: SyslogWrapper(self), (address, port))
        self.loop.run_until_complete(self.coro)

    def handle_event(self, event):
        self.stats['eventsConsumed'] += 1
        asyncio.async(RouterNode.deliver(self, event))

    def decode_error(self, str):
        self.stats['failedConsumed'] += 1

    def overflow(self):
        self.stats['failedConsumed'] += 1

class MemoryPipe(RouterNode):
    def __init__(self, capacity):
        RouterNode.__init__(self)
        capacity=int(capacity)
        self.pipe = asyncio.Queue(capacity) 
        self.stats.update({"maxCapacity": capacity})
        self.dequeuer = asyncio.async(self.dequeue())

    @asyncio.coroutine
    def consume(self, event):
        try:
            self.pipe.put_nowait(event)
        except asyncio.QueueFull:
            self.stats['failedConsumed'] += 1
            return False
        self.stats['eventsConsumed'] += 1
        return True

    def dequeue(self):
        while True:
            event = yield from self.pipe.get()
            yield from RouterNode.deliver(self, event) 

    def statistics(self):
        stats = self.stats
        stats['capacity'] = self.pipe.qsize()
        return stats


class PrinterSink(RouterNode):
    def __init__(self):
        RouterNode.__init__(self)
        self.stats = {"eventsConsumed": 0} 

    @asyncio.coroutine
    def consume(self, event):
        self.stats['eventsConsumed'] += 1
        print(event)
        return True
 
class HTTPSink(RouterNode):
    def __init__(self, uri, batchsize=100, batchwait=10, n_clients=20, timeout=5):
        RouterNode.__init__(self)
        self.sem = asyncio.Semaphore(int(n_clients))
        self.session = aiohttp.ClientSession()
        self.uri = uri
        self.timeout = float(timeout)
        self.batchsize = int(batchsize)
        self.batchwait = int(batchwait)
        self.events = []
        self.handle = None

    @asyncio.coroutine
    def _handle_response(self, resp, len):
        try:
            r = yield from resp
            yield from r.text()
        except aiohttp.errors.ClientOSError:
            self.stats['failedDelivered'] += len
            self.sem.release()
            return False
        except asyncio.TimeoutError:
            self.stats['failedDelivered'] += len
            self.sem.release()
            return False
        except asyncio.CancelledError:
            self.sem.release()
            return False
        self.sem.release()
        if r.status == 202:
            self.stats['eventsDelivered'] += len
            return True
        else:
            self.stats['failedDelivered'] += len
            return False

    def _send_events(self):
        yield from self.sem.acquire()
        try:
            events = self._convert_events(self.events)
            r = asyncio.wait_for(self.session.request('POST', self.uri, 
                                 data=events), self.timeout)
        except asyncio.CancelledError:
            self.sem.release()
            return False
            
        asyncio.async(self._handle_response(r, len(self.events)))
        self.events = []
        if self.handle is not None:
            self.handle.cancel()
            self.handle = None
        return True

    @asyncio.coroutine
    def _flush_batch(self, wait):
        yield from asyncio.sleep(wait)
        yield from self._send_events()

    def _convert_events(self, events):
        return json.dumps(events)

    @asyncio.coroutine
    def consume(self, event):
        self.stats['eventsConsumed'] += 1
        self.events.append(event)
        if len(self.events) > self.batchsize:
            return self._send_events()
        else:
            if self.handle is None:
                self.handle = asyncio.async(self._flush_batch(self.batchwait))
 
        return True

class FlumeHTTPSink(HTTPSink):
    def __init__(self, uri, batchsize=100, batchwait=10, n_clients=20, timeout=5):
        HTTPSink.__init__(self, uri, batchsize, batchwait, n_clients, timeout)

    def _convert_events(self, events):
        return json.dumps([{"headers": event, "body": "none"}  \
                           for event in events])

class HTTPSource(RouterNode):
    def __init__(self, address, port):
        RouterNode.__init__(self)
        self.app = aiohttp.web.Application()
        self.app.router.add_route('POST', '/', self.post)
        loop = asyncio.get_event_loop()
        server = loop.create_server(self.app.make_handler(), address, int(port))
        loop.run_until_complete(server)

    @asyncio.coroutine
    def post(self, request):
        try:
            events = yield from request.json()
            for event in events:
                event['http_path'] = request.path
                event['http_source'] = request.transport.get_extra_info('peername')
                self.stats['eventsConsumed'] += 1
                yield from RouterNode.deliver(self, event)
            return aiohttp.web.HTTPAccepted()
        except ValueError:
            self.stats['failedConsumed'] += 1
            return aiohttp.web.HTTPBadRequest()

class StaticHeader(RouterNode):
    def __init__(self, header, value):
        RouterNode.__init__(self)
        self.header = {header: value}

    @asyncio.coroutine
    def consume(self, event):
        event.update(self.header)
        yield from RouterNode.deliver(self, event)
