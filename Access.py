import pandas as pd
import numpy as np
import json
import paramiko
from datetime import datetime, timedelta
import os

# SSH Configuration
SSH_HOST = "172.23.16.68"
SSH_PORT = 22
SSH_USERNAME = "kienpt"
SSH_PASSWORD = "l5#=;zXIa12'lt&%"
NUM_PROCESSES = 4

def read_json(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    rrd_info = []

    for region in data:
        region_data = data[region]
        for location, rings in region_data.items():
            for ring_name, nodes in rings.items():
                for node_name, entries in nodes.items():
                    for entry in entries:
                        rrd_info.append((
                            entry.get("rrd", "").strip(),
                            region,               # Location (MB/MN/MT)
                            ring_name,            # CO
                            node_name,            # device
                            entry.get("Device"),  # device_cr (optional)
                            entry.get("Type"),    # rrd_type
                            entry.get("Burstable", 0),
                            entry.get("Commit", 0)
                        ))
    return rrd_info

def access_file(args):
    rra_file, start_ts, stop_ts = args
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(SSH_HOST, SSH_PORT, SSH_USERNAME, SSH_PASSWORD)
        cmd = f"rrdtool fetch /var/www/html/cacti/{rra_file} AVERAGE --start {start_ts} --end {stop_ts}"
        _, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode()
        error = stderr.read().decode()

        if error:
            print(f"RRDTool Error for {rra_file}: {error}")
            return rra_file, None

        return rra_file, result

    except Exception as e:
        print(f"SSH Error for {rra_file}: {e}")
        return rra_file, None

    finally:
        ssh.close()

def process_rrd_data(data):
    lines = data.splitlines()
    records = []
    for line in lines[1:]:
        parts = line.split()
        if len(parts) >= 3 and ":" in parts[0]:
            try:
                ts = int(parts[0].replace(":", ""))
                values = [float(x) if x.lower() != "nan" else 0 for x in parts[1:3]]
                records.append([ts] + values)
            except ValueError:
                continue
    df = pd.DataFrame(records, columns=["timestamp", "traffic_in", "traffic_out"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["traffic_in"] = df["traffic_in"] * 8 / 1e6 / 1024
    df["traffic_out"] = df["traffic_out"] * 8 / 1e6 / 1024
    return df

def analyze_performance(json_file_path, time_start, time_end):
    rrd_files = read_json(json_file_path)
    start_ts = int(datetime.combine(time_start, datetime.min.time()).timestamp())
    stop_ts = int(datetime.combine(time_end, datetime.max.time()).timestamp())
    args = [(f[0], start_ts, stop_ts) for f in rrd_files]

    results = [access_file(arg) for arg in args]

    ring_data = {}
    for idx, (rrd_file, data) in enumerate(results):
        metadata = rrd_files[idx]
        if not data:
            continue
        df = process_rrd_data(data)
        if df.empty:
            continue
        rrd_path, location, pop, device, device_cr, rrd_type, burstable, commit = metadata
        ring_data[device] = {
            "df": df,
            "commit": commit,
            "CO": pop,
            "Location": location,
            "device_cr": device_cr
        }

    summary = []
    co_traffic = {}

    for ring_name, info in ring_data.items():
        df = info["df"]
        commit = info["commit"]
        location = info["Location"]
        pop = info["CO"]
        device_cr = info["device_cr"]

        values_in = df["traffic_in"].dropna()
        values_out = df["traffic_out"].dropna()

        if values_in.empty or values_out.empty:
            continue

        p95_in = np.percentile(values_in, 95)
        p95_out = np.percentile(values_out, 95)
        max_in = values_in.max()
        max_out = values_out.max()

        if pop not in co_traffic:
            co_traffic[pop] = {
                "sum_max_in": 0,
                "sum_max_out": 0
            }

        co_traffic[pop]["sum_max_in"] += max_in
        co_traffic[pop]["sum_max_out"] += max_out

        summary.append({
            "Location": location,
            "CO": pop,
            "device_cr": device_cr,
            "Ring": ring_name,
            "95% In (Gbps)": round(p95_in, 2),
            "95% Out (Gbps)": round(p95_out, 2),
            "Max In (Gbps)": round(max_in, 2),
            "Max Out (Gbps)": round(max_out, 2),
            "SUM Max IN (Gbps)": round(co_traffic[pop]["sum_max_in"], 2),
            "SUM Max OUT (Gbps)": round(co_traffic[pop]["sum_max_out"], 2),
            "Capacity (Gbps)": commit,
            "Hiệu suất In (%)": round((max_in / commit) * 100, 1) if commit else 0,
            "Hiệu suất Out (%)": round((max_out / commit) * 100, 1) if commit else 0
        })

    df_result = pd.DataFrame(summary)
    return df_result

# MAIN
if __name__ == "__main__":
    json_path = "BW_Access.json"
    start_date = (datetime.now() - timedelta(days=7)).date()
    end_date = datetime.now().date()

    if os.path.exists(json_path) and start_date <= end_date:
        print("🚀 Processing data...")
        df_summary = analyze_performance(json_path, start_date, end_date)

        if not df_summary.empty:
            print("✅ Analysis complete. Saving to rrd_ring_summary.csv")
            df_summary.to_csv("rrd_ring_summary.csv", index=False)
            print(df_summary)
        else:
            print("⚠️ No usable data found in RRD.")
    else:
        print("❌ Invalid input: JSON file missing or date range incorrect.")
