import aiohttp
import asyncio

class Agent():
    def __init__(self, name):
        self.name = name
        self.nodes = {}

    def addNode(self, name, node):
        self.nodes[name] = node

    def connectNode(self, node1, node2):
        self.nodes[node1].addConsumer(self.nodes[node2])

    def getNodes(self):
        return self.nodes.keys()

class HTTPStatisticsReporter():
    def __init__(self, address, port, agent):
        pass

    @asyncio.coroutine
    def get(self, request):
        pass


