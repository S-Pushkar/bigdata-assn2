#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
import sys
import math

topic = sys.argv[2]

consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')

competitions = {}

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))

    if data[0] == 'EOF':
        break

    competition_id = data[1]
    user_id = data[2]
    com_problem_id = data[3]
    category = data[4]
    difficulty = data[5]
    com_submission_id = data[6]
    status = data[7]
    language = data[8]
    runtime = int(data[9])
    time_taken = int(data[10])

    if competition_id not in competitions:
        competitions[competition_id] = {}

    if user_id not in competitions[competition_id]:
        competitions[competition_id][user_id] = 0

    status_score = 0

    if status == 'Passed':
        status_score = 100
    elif status == 'TLE':
        status_score = 20
    else:
        status_score = 0

    difficulty_score = 0

    if difficulty == 'Easy':
        difficulty_score = 1
    elif difficulty == 'Medium':
        difficulty_score = 2
    else:
        difficulty_score = 3

    bonus = max(1, (1 + (10000 / runtime) - (0.25 * time_taken)))

    competitions[competition_id][user_id] += (status_score * difficulty_score * bonus)

competitions = dict(sorted(competitions.items(), key=lambda x: x[0]))

for competition_id in competitions:
    competitions[competition_id] = dict(sorted(competitions[competition_id].items(), key=lambda x: x[0]))
    for user_id in competitions[competition_id]:
        competitions[competition_id][user_id] = math.floor(competitions[competition_id][user_id])

print(json.dumps(competitions, indent=4))
