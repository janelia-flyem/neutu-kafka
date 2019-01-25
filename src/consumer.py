import argparse
import sys
from kafka import KafkaConsumer
from datetime import datetime
import time
from pprint import pprint
import json
from termcolor import colored

interact_count = 0
# all_mouse_stat = {}
# all_key_stat = {}
all_object_stat = {}

def get_detail(message):
    user = message.get("user", "*")
    detail = "; ".join([s for s in [message.get("description", ""), message.get("diagnostic"), 
        # format_mouse_message(message, user), format_key_message(message, user),
        format_object_message(message, user)] if s])

    return detail

def get_timestamp(s, format = '%Y-%m-%d'):
    return int(time.mktime(datetime.strptime(s, format).timetuple()))

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

    if ARGS.action:
        action = message.get("action", "")
        if action:
            cond = cond and (action == ARGS.action)
        else:
            cond = False

    if ARGS.window:
        window = message.get("window", "")
        if window:
            cond = cond and (window == ARGS.window)
        else:
            cond = False

    if ARGS.obj:
        obj = message.get("object", "")
        # print(message)
        if obj and isinstance(obj, dict):
            obj_type = obj.get("type", "")
            obj_name = obj.get("name", "")
            obj_id = obj.get("id", "")
            obj_str = obj_type + '/' + obj_name + '/' + obj_id
            # print(obj_str, ARGS.obj)
            cond = cond and (obj_str.startswith(ARGS.obj))
        else:
            cond = False

    if ARGS.description:
        description = message.get("description", "")
        if description:
            cond = cond and (ARGS.description.lower() in description.lower())
        else:
            cond = False
        #print(cond)

    if ARGS.detail:
        detail = get_detail(message)
        if detail:
            cond = cond and (ARGS.detail.lower() in detail.lower())
        else:
            cond = False

    if ARGS.date:
        timestamp = message.get("time", "")
        if timestamp:
            try:
                d = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                cond = cond and (d == ARGS.date)
            except:
                cond = False
        else:
            cond = False

    #cond = cond and ("url" in message)

    return cond

def update_object_stat(obj, action, user):
    # print(obj)
    if not user in all_object_stat:
        all_object_stat[user] = {}
    obj_stat = all_object_stat[user]
    s = obj.get("type", "") + " " + obj.get("name", "")  + ": " + action
    if s in obj_stat:
        obj_stat[s] += 1
    else:
        obj_stat[s] = 1

    return s + ' x' + str(obj_stat[s])

def format_object_message(message, user = '*'):
    obj = message.get("object", {})
    # print(obj)
    if obj:
        return update_object_stat(obj, message.get("action", ""), user)


def print_message(message):
    # print(message)
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
        print (datetime.fromtimestamp(message['time']).strftime('%Y-%m-%d %H:%M:%S'), message.get("user", "")+" |", 
            colored(get_detail(message), "green"))
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
    
    time_range = [None, None]
    if ARGS.date:
        time_range[0] = get_timestamp(ARGS.date)
        time_range[1] = time_range[0] + 86400

    # for message in consumer:
    #     value = json.loads(message.value.decode('utf-8'))
    #     if passed(value):
    #         print_message(value)

    for message in consumer:
        t = int(message.timestamp / 1000)
        # print(t, time_range[0], time_range[1])
        range_state = 1
        if time_range[0] and t < range_state:
            range_state = 0
        if time_range[1] and t >= time_range[1]:
            range_state = 2

        if range_state == 1:
            value = json.loads(message.value.decode('utf-8'))
            if passed(value):
                print_message(value)
        elif range_state == 2:
            break

def save_stat(output):
    with open(output, "w") as fp:
        if all_object_stat:
            print('\nSaving statstics in', output, '...')
            json.dump(stat, fp, indent=2)
            print('Done!')
        else:
            print("\nNo statistics to be saved.")

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
    PARSER.add_argument('--category', dest='category', default='', help='Category')
    PARSER.add_argument('--group', dest='group', default='', help='Group')
    PARSER.add_argument('--user', dest='user', help='User')
    PARSER.add_argument('--description', dest='description', help='Description')
    PARSER.add_argument('--detail', dest='detail', help='Detail')
    PARSER.add_argument('--object', dest='obj', help='Object')
    PARSER.add_argument('--action', dest='action', help='Action')
    PARSER.add_argument('--window', dest='window', help='Window')
    PARSER.add_argument('--offset', dest='offset', default='latest',
                        help='offset (earliest or latest)')
    PARSER.add_argument('--debug', dest='debug', action='store_true',
                        default=False, help='Flag, Very chatty')
    PARSER.add_argument('--date', dest='date', help='Date')
    PARSER.add_argument('--output', dest='output', help='Output')

    ARGS = PARSER.parse_args()
    try:
        read_messages()
    except Exception as e:
        print(e)
    except:
        pass
        
    if ARGS.output:
        save_stat(ARGS.output)
    else:
        print("\n")
        print(all_object_stat)


    # print(json.dumps(all_mouse_stat, sort_keys=True))
    sys.exit(0)
