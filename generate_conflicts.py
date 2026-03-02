import json

# Channel mapping
channels = {
    1: 'Ph1',
    2: 'Ph2',
    3: None,
    4: 'Ph4',
    5: 'Ph5',
    6: 'Ph6',
    7: None,
    8: 'Ph8',
    9: 'O1',
    10: 'O2',
    11: 'O3',
    12: 'O4',
    13: 'Ped2',
    14: 'Ped4',
    15: 'Ped6',
    16: 'Ped8'
}

# List of removed diodes (allowable pairs)
removed_diodes = [
    (8, 9),
    (2, 5), (6, 9),
    (1, 5), (2, 6), (5, 9), (6, 10),
    (1, 6), (6, 11),
    (2, 8), (4, 10), (5, 11), (6, 12),
    (2, 9), (4, 11), (5, 12), (6, 13),
    (2, 10), (5, 13), (8, 16),
    (2, 11), (6, 15), (9, 16),
    (2, 12), (4, 14), (9, 15),
    (2, 13), (10, 15),
    (11, 15), (10, 14), (9, 13),
    (2, 15), (12, 15), (11, 14), (10, 13), (9, 12),
    (13, 15), (10, 12), (9, 11),
    (13, 14), (12, 13), (11, 12), (10, 11), (9, 10)
]

# Generate all possible pairs of active channels
active_channels = [ch for ch, name in channels.items() if name is not None]
all_pairs = []
for i in range(len(active_channels)):
    for j in range(i + 1, len(active_channels)):
        ch1 = active_channels[i]
        ch2 = active_channels[j]
        # Ensure smaller channel number is first
        pair = (min(ch1, ch2), max(ch1, ch2))
        all_pairs.append(pair)

# Find conflict pairs (present diodes)
conflict_pairs = []
for pair in all_pairs:
    if pair not in removed_diodes:
        name1 = channels[pair[0]]
        name2 = channels[pair[1]]
        conflict_pairs.append([name1, name2])

# Custom JSON formatting to put each pair on a single line
json_str = '{\n    "12059": [\n'
pairs_str = []
for pair in conflict_pairs:
    pairs_str.append(f'        ["{pair[0]}", "{pair[1]}"]')
json_str += ',\n'.join(pairs_str)
json_str += '\n    ]\n}\n'

with open('firmware_validation/conflict_monitor/conflict_pairs.json', 'w') as f:
    f.write(json_str)

print(f"Generated {len(conflict_pairs)} conflict pairs.")
