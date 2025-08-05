import json
import os

input_json_path = '../input_data/2025-02-01_060201.Sentinel_IncidentSync__ts_only.json'

output_folder = 'test_data_json'

items_per_file = 1000
max_files = 100

os.makedirs(output_folder, exist_ok=True)

with open(input_json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

total_items = len(data)

for i in range(max_files):
    start_index = i * items_per_file
    end_index = start_index + items_per_file

    chunk = data[start_index:end_index]

    if not chunk:
        break

    output_path = os.path.join(output_folder, f'part_{i+1:03d}.json')

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(chunk, f, ensure_ascii=False, indent=2)
