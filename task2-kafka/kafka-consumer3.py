#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
import sys

topic = sys.argv[3]

consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')

contributors = {}

user_elo_ratings = {}

for message in consumer:
    data = json.loads(message.value.decode())

    if data[0] == 'EOF':
        break

    if data[0] == 'solution':
        user_id = data[1]
        upvotes = int(data[4])

        if user_id not in contributors:
            contributors[user_id] = 0

        contributors[user_id] += upvotes

        continue

    user_id = ''
    status = ''
    difficulty = ''
    runtime = 0

    if data[0] == 'problem':
        user_id = data[1]
        status = data[6]
        difficulty = data[4]
        runtime = int(data[8])
    elif data[0] == 'competition':
        user_id = data[2]
        status = data[7]
        difficulty = data[5]
        runtime = int(data[9])

    if user_id not in user_elo_ratings:
        user_elo_ratings[user_id] = 1200

    k = 32

    status_score = 0

    if status == 'Passed':
        status_score = 1
    elif status == 'TLE':
        status_score = 0.2
    else:
        status_score = -0.3

    difficulty_score = 0

    if difficulty == 'Easy':
        difficulty_score = 0.3
    elif difficulty == 'Medium':
        difficulty_score = 0.7
    else:
        difficulty_score = 1

    runtime_bonus = 10000 / runtime

    user_elo_ratings[user_id] += k * (status_score * difficulty_score) + runtime_bonus

contributors = sorted(contributors.items(), key=lambda x: x[1], reverse=True)

max_contribution = contributors[0][1]

output = {}

output['best_contributor'] = []

for user_id, contribution in contributors:
    if contribution == max_contribution:
        output['best_contributor'].append(user_id)
    else:
        break

output['best_contributor'] = sorted(output['best_contributor'])

output['user_elo_rating'] = dict(sorted(user_elo_ratings.items(), key=lambda x: x[0]))

for user_id, elo_rating in output['user_elo_rating'].items():
    output['user_elo_rating'][user_id] = round(elo_rating)

print(json.dumps(output, indent=4))
