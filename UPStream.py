import pandas as pd
import numpy as np
import json
import paramiko
import rrdtool
from datetime import datetime, timedelta
import multiprocessing
import argparse
import os

# === Cấu hình SSH & xử lý song song ===
SSH_HOST = "172.28.131.72"
SSH_PORT = 22
SSH_USERNAME = "kienpt"
SSH_PASSWORD = "l5#=;zXIa12'lt&%"
NUM_PROCESSES = 4

# === Đọc cấu hình JSON ===
def read_json(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    rrd_info = []
    for key in ["HKG", "SGP", "JPN"]:
        for entry in data.get(key, []):
            if isinstance(entry, dict) and "rrd" in entry:
                rrd_info.append((
                    entry["rrd"],
                    entry.get("Device"),
                    entry.get("Type", ""),
                    entry.get("Burstable_Mbps", 0),
                    entry.get("Commit_Mbps", 0),
                    key
                ))
    return rrd_info

# === Kết nối SSH và lấy dữ liệu RRD ===
def access_file(args):
    rra_file, time_start, time_stop = args
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(SSH_HOST, SSH_PORT, SSH_USERNAME, SSH_PASSWORD)
        cmd = f"rrdtool fetch /var/www/html/cacti/rra/{rra_file} AVERAGE --start {time_start} --end {time_stop}"
        _, stdout, _ = ssh.exec_command(cmd)
        return rra_file, stdout.read().decode()
    except Exception as e:
        print(f"[ERROR] {rra_file}: {e}")
        return rra_file, None
    finally:
        ssh.close()

# === Xử lý dữ liệu RRD (chuyển về DataFrame) ===
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
            except:
                continue
    df = pd.DataFrame(records, columns=["timestamp", "traffic_in", "traffic_out"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["traffic_in"] = df["traffic_in"] * 8 / 1e6 / 1024  # bps → Gbps
    df["traffic_out"] = df["traffic_out"] * 8 / 1e6 / 1024
    return df

# === Lấy dữ liệu từ nhiều RRD cùng lúc ===
def fetch_all_data(rrd_files, start_r, stop_r):
    args = [(f[0], start_r, stop_r) for f in rrd_files]
    with multiprocessing.Pool(NUM_PROCESSES) as pool:
        return pool.map(access_file, args)

# === Tổng hợp dữ liệu theo thiết bị ===
def aggregate_data(results, rrd_files):
    ring_data = {}
    for idx, (rrd_file, data) in enumerate(results):
        if not data:
            continue
        df = process_rrd_data(data)
        if df.empty:
            continue
        rrd_path, device, rrd_type, burstable, commit, pop = rrd_files[idx]
        ring_data[device] = {
            "df": df,
            "burstable": burstable,
            "commit": commit,
            "pop": pop
        }
    return ring_data

# === Tính toán hiệu suất & thống kê ===
def summarize(ring_data):
    summary = []
    for device, info in ring_data.items():
        df = info["df"]
        burstable = info["burstable"]
        commit = info["commit"]
        pop = info["pop"]

        capacity = (burstable + commit) / 1024 or 1  # Mbps → Gbps
        values_in = df["traffic_in"].dropna()
        values_out = df["traffic_out"].dropna()

        if values_in.empty or values_out.empty:
            print(f"⚠️ Không có dữ liệu hợp lệ cho thiết bị {device} ({pop}), bỏ qua.")
            continue

        p95_in = np.percentile(values_in, 95)
        p95_out = np.percentile(values_out, 95)
        max_in = values_in.max()
        max_out = values_out.max()

        summary.append({
            "POP": pop,
            "Device": device,
            "95% In (Gbps)": round(p95_in, 2),
            "95% Out (Gbps)": round(p95_out, 2),
            "Max In (Gbps)": round(max_in, 2),
            "Max Out (Gbps)": round(max_out, 2),
            "Capacity (Gbps)": round(capacity, 2),
            "Hiệu suất In (%)": round((max_in / capacity) * 100, 1),
            "Hiệu suất Out (%)": round((max_out / capacity) * 100, 1)
        })
    return pd.DataFrame(summary)

# === Hàm main điều phối chương trình ===
def main(json_path, time_start, time_end):
    print("🚀 Bắt đầu phân tích...")

    rrd_files = read_json(json_path)
    start_r = int(datetime.combine(time_start, datetime.min.time()).timestamp())
    stop_r = int(datetime.combine(time_end, datetime.max.time()).timestamp())

    results = fetch_all_data(rrd_files, start_r, stop_r)
    ring_data = aggregate_data(results, rrd_files)
    df_sum = summarize(ring_data)

    print("\n✅ Phân tích hoàn tất. Kết quả tổng hợp:\n")
    print(df_sum.to_string(index=False))

# === Xử lý đối số dòng lệnh ===
if __name__ == "__main__":
    multiprocessing.freeze_support()

    parser = argparse.ArgumentParser(description="Phân tích hiệu suất Ring từ RRD")
    parser.add_argument("--json", type=str, default="BW_Upstream.json", help="Đường dẫn tới file JSON cấu hình")
    parser.add_argument("--start", type=str, help="Ngày bắt đầu (YYYY-MM-DD)",
                        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
    parser.add_argument("--end", type=str, help="Ngày kết thúc (YYYY-MM-DD)",
                        default=datetime.now().strftime("%Y-%m-%d"))

    args = parser.parse_args()
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    main(args.json, start_date, end_date)
