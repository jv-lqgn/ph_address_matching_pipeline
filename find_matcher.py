import json
import sys

with open(r"address_matching\notebooks\drafts\draft_1(fzymch ver).ipynb") as f:
    nb = json.load(f)
    
for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'code' and isinstance(cell['source'], list):
        code = ''.join(cell['source'])
        if 'def match_address' in code:
            print(f"Cell index: {i}")
            print(f"Cell ID: {cell.get('id', 'N/A')}")
            print(f"Code length: {len(code)}")
            # Find the line where the logic needs to be fixed
            lines = code.split('\n')
            for j, line in enumerate(lines):
                if 'elif len(city_candidates) > 1' in line or 'len(city_candidates) == 1' in line:
                    print(f"\nFound at line {j}: {line}")
                    for k in range(max(0, j-2), min(len(lines), j+8)):
                        print(f"  {k:3d}: {lines[k]}")
            
            # Also print the area where it checks city_candidates at all
            for j, line in enumerate(lines):
                if 'city_candidates' in line and ('len(' in line or 'in city_candidates' in line):
                    print(f"\nContext around line {j}: {line[:70]}")
                    for k in range(max(0, j-1), min(len(lines), j+3)):
                        print(f"  {k:3d}: {lines[k][:100]}")
            break
