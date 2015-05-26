import configparser
import logging
from pkg_resources import iter_entry_points
from pylogrouter import Agent


def spawn_agent(name, agent_cfg, cfg):
    logging.info("Spawning agent {}".format(name))
    a = Agent(name)
    for node in agent_cfg['nodes'].split():
        nodecfg = cfg[node]
        type = nodecfg['type']
        del nodecfg['type']
        nodeobj = create_node(type, nodecfg)
        a.add_node(node, nodeobj)
        logging.info("  Added node {} ({})".format(node, type))
    for node in agent_cfg['nodes'].split():
        try:
            consumers = agent_cfg["{}.consumers".format(node)].split()
        except KeyError:
            continue
        for consumer in consumers:
            logging.info("  connecting {} to {}".format(node, consumer))
            a.connect_node(node, consumer)
    return a
        

def parse_config(file):
    cfg = configparser.ConfigParser()
    cfg.read(file)
    keys = cfg.keys()
    agents = []
    for key in keys:
        if key.startswith("agent:"):
            agents.append(spawn_agent(key.split(":")[1], cfg[key], cfg))
    return agents

def create_node(type, args):
    names = iter_entry_points(group="pylogrouter.plugin", name=type)
    for name in names:
        obj = name.load()(**args)
        return obj

    

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Found available plugins:")
    for plugin in iter_entry_points(group="pylogrouter.plugin"):
        logging.info("    {0}".format(plugin.name))

    agents = parse_config('example.cfg')
    agents[0].run()
