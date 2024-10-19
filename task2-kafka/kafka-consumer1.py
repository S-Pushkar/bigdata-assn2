#!/usr/bin/env python3

from kafka import KafkaConsumer
import sys
import json

topic = sys.argv[1]

consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')

languages = {}

category_passed = {}
category_total = {}

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))

    if data[0] == 'EOF':
        break

    user_id = data[1]
    problem_id = data[2]
    category = data[3]
    difficulty = data[4]
    submission_id = data[5]
    status = data[6]
    language = data[7]
    runtime = data[8]

    if language not in languages:
        languages[language] = 0
    languages[language] += 1

    if category not in category_passed:
        category_passed[category] = 0
    if category not in category_total:
        category_total[category] = 0

    category_total[category] += 1

    if status == 'Passed':
        category_passed[category] += 1


output = {}

sorted_languages = sorted(languages.items(), key=lambda x: x[1], reverse=True)

output['most_used_language'] = []
max_language = sorted_languages[0][1]

for language, count in sorted_languages:
    if count == max_language:
        output['most_used_language'].append(language)
    else:
        break

output['most_used_language'] = sorted(output['most_used_language'])

output['most_difficult_category'] = []

category_difficulty = {}

for category in category_passed:
    category_difficulty[category] = category_passed[category] / category_total[category]

sorted_categories = sorted(category_difficulty.items(), key=lambda x: x[1])

min_difficulty = sorted_categories[0][1]

for category, difficulty in sorted_categories:
    if difficulty == min_difficulty:
        output['most_difficult_category'].append(category)
    else:
        break

output['most_difficult_category'] = sorted(output['most_difficult_category'])

print(json.dumps(output, indent=4))
