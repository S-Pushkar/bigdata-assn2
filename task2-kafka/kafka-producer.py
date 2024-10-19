#!/usr/bin/env python3

from kafka import KafkaProducer
import sys
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

consumer1_topic = sys.argv[1]

consumer2_topic = sys.argv[2]

consumer3_topic = sys.argv[3]

for l in sys.stdin:
    line = l.strip().split(" ")

    if line[0] == 'problem':
        producer.send(consumer1_topic, json.dumps(line).encode('utf-8'))
        producer.send(consumer3_topic, json.dumps(line).encode('utf-8'))
    elif line[0] == 'competition':
        producer.send(consumer2_topic, json.dumps(line).encode('utf-8'))
        producer.send(consumer3_topic, json.dumps(line).encode('utf-8'))
    elif line[0] == 'solution':
        producer.send(consumer3_topic, json.dumps(line).encode('utf-8'))
    else:
        producer.send(consumer1_topic, json.dumps(line).encode('utf-8'))
        producer.send(consumer2_topic, json.dumps(line).encode('utf-8'))
        producer.send(consumer3_topic, json.dumps(line).encode('utf-8'))

producer.flush()
