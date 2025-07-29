import re 
import json 
import pprint
from itertools import zip_longest

def Json_reader(name):
    with open(name, 'r', encoding="utf-8") as f:
        return json.load(f)

def same_pos_digt(s1, s2):
    d1_digits = ''.join(re.findall(r'\d+', s1))
    d2_digits = ''.join(re.findall(r'\d+', s2))
    return d2_digits.startswith(d1_digits)

def processing_path():
    path_map = {}
    with open("Raw.txt", 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('|')
            parts = [p.strip() for p in parts if p.strip()]
            if len(parts) >= 2:
                id_str = parts[0]
                path = parts[1]
                try:
                    id_int = int(id_str)
                    path_map[id_int] = path
                except ValueError:
                    continue
    return path_map

def main(path="MIEN_BAC.json"):
    data = Json_reader(path)
    id_to_path = processing_path()

    key_map = {
        "MB": "Mien_Bac",
        "MN": "Mien_Nam",
        "MT": "Mien_Trung",
    }

    results = {}

    for region_key, mien_field in key_map.items():
        print(f"\nProcessing region: {region_key}")
        region = data.get(region_key, {}) or {}

        mien     = region.get(mien_field, []) or []
        devices  = region.get("Name_Device", []) or []
        ma01     = region.get("MA01", []) or []
        ma02     = region.get("MA02", []) or []

        indexed_matches = []
        for i, (m, d) in enumerate([(m, d) for m in mien for d in devices if same_pos_digt(m, d)]):
            a = ma01[i] if i < len(ma01) else None
            b = ma02[i] if i < len(ma02) else None

            path_a = id_to_path.get(a, "N/A") if a is not None else "N/A"
            path_b = id_to_path.get(b, "N/A") if b is not None else "N/A"

            indexed_matches.append({
                "mien": m,
                "device": d,
                "path_ma01": path_a,
                "path_ma02": path_b
            })

        results[region_key] = {
            "matches": indexed_matches
        }

    pprint.pprint(results)
    return results

def Generation_Json(results, output_file="BW_OLT.json"):
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    print(f"Generated {output_file}")

if __name__ == "__main__":
    results = main()
    Generation_Json(results)
