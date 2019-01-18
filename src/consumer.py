import argparse
import sys
from kafka import KafkaConsumer
from datetime import datetime
from pprint import pprint
import json
from termcolor import colored

interact_count = 0

def passed(message):
    cond = True
    if ARGS.category:
        #print(value)
        category = message.get("category", "")
        if category:
            cond = cond and (category == ARGS.category)
        else:
            cond = False

    if ARGS.user:
        user = message.get("user", "")
        if user:
            cond = cond and (user == ARGS.user)
        else:
            cond = False

    if ARGS.description:
        description = message.get("description", "")
        #print(ARGS.description.lower(), description.lower())
        if description:
            cond = cond and (ARGS.description.lower() in description.lower())
        else:
            cond = False
        #print(cond)

    #cond = cond and ("url" in message)

    return cond

def print_message(message):
    #print(message)
    #print (datetime.fromtimestamp(message['time']).strftime('[%Y-%m-%d %H:%M:%S] '), message)
    category = message.get("category", "")
    if category == 'interact':
        global interact_count
        interact_count += 1
        print('#interaction:', interact_count)
    elif category == 'profile':
        duration = message.get("duration", 0)
        if duration > 100:
            print (datetime.fromtimestamp(message['time']).strftime('[%Y-%m-%d %H:%M:%S] '), message)
    elif category == 'INFO':
        print (datetime.fromtimestamp(message['time']).strftime('%Y-%m-%d %H:%M:%S'), message.get("user", "")+" |", colored(message.get("description", ""), "green"))
    elif category == 'ERROR':
        print (datetime.fromtimestamp(message['time']).strftime('%Y-%m-%d %H:%M:%S'), message.get("user", "")+" |", colored(message.get("description", ""), "red"))
    else:
        print (datetime.fromtimestamp(message['time']).strftime('[%Y-%m-%d %H:%M:%S] '), message)




def read_messages():
    if ARGS.server:
        server_list = [ARGS.server + ':9092']
    else:
        server_list = ['kafka.int.janelia.org:9092', 'kafka2.int.janelia.org:9092', 'kafka3.int.janelia.org:9092']
    if not ARGS.group:
        ARGS.group = None
    consumer = KafkaConsumer(ARGS.topic,
                             bootstrap_servers=server_list,
                             group_id=ARGS.group,
                             auto_offset_reset=ARGS.offset)
    for message in consumer:
        value = json.loads(message.value.decode('utf-8'))
        if passed(value):
            print_message(value)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
    PARSER.add_argument('--category', dest='category', default='', help='Category')
    PARSER.add_argument('--group', dest='group', default='', help='Group')
    PARSER.add_argument('--user', dest='user', help='User')
    PARSER.add_argument('--description', dest='description', help='Description')
    PARSER.add_argument('--offset', dest='offset', default='latest',
                        help='offset (earliest or latest)')
    PARSER.add_argument('--debug', dest='debug', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARGS = PARSER.parse_args()
    read_messages()
    sys.exit(0)
