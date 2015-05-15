import aiohttp.web
import asyncio
import json

class Agent():
    def __init__(self, name):
        self.name = name
        self.nodes = {}

    def add_node(self, name, node):
        self.nodes[name] = node

    def connect_node(self, node1, node2):
        self.nodes[node1].add_consumer(self.nodes[node2])

    def get_nodes(self):
        return self.nodes.keys()

    def get_node(self, name):
        return self.nodes[name]

class HTTPStatisticsReporter():
    def __init__(self, address, port, agent):
        self.agent = agent
        self.app = aiohttp.web.Application()
        self.app.router.add_route('GET', '/', self.get)
        loop = asyncio.get_event_loop()
        server = loop.create_server(self.app.make_handler(), address, port)
        loop.run_until_complete(server)

    @asyncio.coroutine
    def get(self, request):
        nodes = {node: self.agent.get_node(node).statistics() 
                   for node in self.agent.get_nodes()}
        for task in asyncio.Task.all_tasks():
            task.print_stack()
        return aiohttp.web.Response(body=bytes(json.dumps(nodes), 'utf-8'))


