import json 

data = json.load(open('backend/data/delhi_ward_elevation.json'))
tc_counts = {}
for d in data:
    c = d['terrain_class']
    tc_counts[c] = tc_counts.get(c, 0) + 1

print('Terrain class counts:', tc_counts)
elevs = [d['mean_elevation'] for d in data]
print(f'Elevation range: {min(elevs)} - {max(elevs)} m')
print(f'Mean elevation: {sum(elevs)/len(elevs):.1f} m')

# Show floodplain wards
fp = [d for d in data if d['terrain_class'] == 'floodplain']
print(f'\nFloodplain wards ({len(fp)}):')
for d in fp:
    print(f'  elev={d["mean_elevation"]}m  lat={d["lat"]} lon={d["lon"]}')

low = [d for d in data if d['terrain_class'] == 'low']
print(f'\nLow wards ({len(low)}):')
for d in low:
    print(f'  elev={d["mean_elevation"]}m  lat={d["lat"]} lon={d["lon"]}')
