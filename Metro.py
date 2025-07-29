# import streamlit as st
# import pandas as pd
# import numpy as np
# import json
# import paramiko
# from datetime import datetime, timedelta
# import os

# # SSH Configuration
# SSH_HOST = "172.23.16.68"
# SSH_PORT = 22
# SSH_USERNAME = "kienpt"
# SSH_PASSWORD = "l5#=;zXIa12'lt&%"
# NUM_PROCESSES = 4


# def read_json(json_path):
#     with open(json_path, 'r', encoding='utf-8') as f:
#         data = json.load(f)
#     rrd_info = []
#     for key in ["MB", "MN", "MT"]:
#         domain = data.get(key, {})
#         for ring_name, ring_nodes in domain.items():
#             for node_name, devices in ring_nodes.items():
#                 for entry in devices:
#                     if "rrd" in entry:
#                         rrd_info.append((
#                             entry["rrd"].strip(),
#                             key,
#                             ring_name,
#                             node_name,
#                             entry.get("Device"),
#                             entry.get("Type"),
#                             entry.get("Burstable", 0),
#                             entry.get("Commit", 0)
#                         ))
#     return rrd_info


# def access_file(args):
#     rra_file, time_start, time_stop = args
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     try:
#         ssh.connect(SSH_HOST, SSH_PORT, SSH_USERNAME, SSH_PASSWORD)
#         cmd = f"rrdtool fetch /var/www/html/cacti/rra/{rra_file} AVERAGE --start {time_start} --end {time_stop}"
#         _, stdout, stderr = ssh.exec_command(cmd)
#         result = stdout.read().decode()
#         return rra_file, result
#     except Exception as e:
#         return rra_file, None
#     finally:
#         ssh.close()


# def process_rrd_data(data):
#     lines = data.splitlines()
#     records = []
#     for line in lines[1:]:
#         parts = line.split()
#         if len(parts) >= 3 and ":" in parts[0]:
#             try:
#                 ts = int(parts[0].replace(":", ""))
#                 values = [float(x) if x.lower() != "nan" else 0 for x in parts[1:3]]
#                 records.append([ts] + values)
#             except ValueError:
#                 continue
#     df = pd.DataFrame(records, columns=["timestamp", "traffic_in", "traffic_out"])
#     df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
#     df["traffic_in"] = df["traffic_in"] * 8 / 1e6 / 1024
#     df["traffic_out"] = df["traffic_out"] * 8 / 1e6 / 1024
#     return df


# def analyze_performance(json_file_path, time_start, time_end):
#     rrd_files = read_json(json_file_path)
#     start_ts = int(datetime.combine(time_start, datetime.min.time()).timestamp())
#     stop_ts = int(datetime.combine(time_end, datetime.max.time()).timestamp())
#     args = [(f[0], start_ts, stop_ts) for f in rrd_files]

#     results = [access_file(arg) for arg in args]

#     ring_data = {}
#     for idx, (rrd_file, data) in enumerate(results):
#         metadata = rrd_files[idx]
#         if not data:
#             continue
#         df = process_rrd_data(data)
#         if df.empty:
#             continue
#         rrd_path, location, pop, device, device_cr, rrd_type, burstable, commit = metadata
#         ring_data[device] = {
#             "df": df,
#             "commit": commit,
#             "CO": pop,
#             "Location": location,
#             "device_cr": device_cr
#         }

#     summary = []
#     for ring_name, info in ring_data.items():
#         df = info["df"]
#         commit = info["commit"]
#         location = info["Location"]
#         pop = info["CO"]
#         device_cr = info["device_cr"]

#         values_in = df["traffic_in"].dropna()
#         values_out = df["traffic_out"].dropna()

#         if values_in.empty or values_out.empty:
#             continue

#         p95_in = np.percentile(values_in, 95)
#         p95_out = np.percentile(values_out, 95)
#         max_in = values_in.max()
#         max_out = values_out.max()

#         summary.append({
#             "Location": location,
#             "CO": pop,
#             "device_cr": device_cr,
#             "Ring": ring_name,
#             "95% In (Gbps)": round(p95_in, 2),
#             "95% Out (Gbps)": round(p95_out, 2),
#             "Max In (Gbps)": round(max_in, 2),
#             "Max Out (Gbps)": round(max_out, 2),
#             "Capacity (Gbps)": commit,
#             "Hiệu suất In (%)": round((max_in / commit) * 100, 1) if commit else 0,
#             "Hiệu suất Out (%)": round((max_out / commit) * 100, 1) if commit else 0
#         })

#     return pd.DataFrame(summary)


# st.set_page_config(page_title="Phân tích hiệu suất Ring", layout="wide")
# st.title("📡 Phân tích hiệu suất Ring từ RRD")

# uploaded_file = st.file_uploader("📁 Tải lên file JSON cấu hình", type=["json"])

# col1, col2 = st.columns(2)
# with col1:
#     start_date = st.date_input("📅 Ngày bắt đầu", value=datetime.now() - timedelta(days=7))
# with col2:
#     end_date = st.date_input("📅 Ngày kết thúc", value=datetime.now())

# json_path = None
# if uploaded_file:
#     json_path = f"/tmp/{uploaded_file.name}"
#     with open(json_path, "wb") as f:
#         f.write(uploaded_file.getbuffer())
# elif os.path.exists("BW_Access.json"):
#     json_path = "BW_Access.json"

# if json_path and start_date <= end_date:
#     with st.spinner("🔄 Đang xử lý dữ liệu..."):
#         df_summary = analyze_performance(json_path, start_date, end_date)

#     if not df_summary.empty:
#         st.success("✅ Phân tích hoàn tất!")
#         st.dataframe(df_summary, use_container_width=True)
#         csv = df_summary.to_csv(index=False).encode("utf-8")
#         st.download_button("📥 Tải kết quả CSV", data=csv, file_name="rrd_ring_summary.csv", mime="text/csv")
#     else:
#         st.warning("⚠️ Không có dữ liệu nào được xử lý.")
# else:
#     st.info("📌 Vui lòng tải lên file JSON hoặc đảm bảo BW_Access.json có tồn tại.")
import streamlit as st 
import pandas as pd
import numpy as np
import json
import paramiko
import rrdtool
import time
from datetime import datetime, timedelta
import multiprocessing

SSH_HOST = "172.28.131.72"
SSH_PORT = 22
SSH_USERNAME = "kienpt"
SSH_PASSWORD = "l5#=;zXIa12'lt&%"
NUM_PROCESSES = 2

@st.cache_data
def read_json(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    rrd_info = []
    for key in ["MB", "MN", "MT"]:
        for ring, devices in data.get(key, {}).items():
            for entry in devices:
                if "rrd" in entry:
                    rrd_info.append((
                        entry["rrd"],
                        entry.get("Device"),
                        entry.get("Type"),
                        entry.get("Burstable", 0),
                        entry.get("Commit", 0),
                        ring + key
                    ))
    print(rrd_info)
    return rrd_info

def access_file(args):
    rra_file, time_start, time_stop = args
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(SSH_HOST, SSH_PORT, SSH_USERNAME, SSH_PASSWORD)
        cmd = f"rrdtool fetch /var/www/html/cacti/{rra_file} AVERAGE --start {time_start} --end {time_stop}"
        _, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode()
        return rra_file, result
    except Exception as e:
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
            except:
                continue
    df = pd.DataFrame(records, columns=["timestamp", "traffic_in", "traffic_out"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["traffic_in"] = df["traffic_in"] * 8 / 1e6 / 1024  # Convert to Gbps
    df["traffic_out"] = df["traffic_out"] * 8 / 1e6 / 1024
    return df

def main():
    st.title("📶 Báo cáo Hiệu suất Ring theo Miền")
    json_file = st.file_uploader("Tải file JSON cấu hình", type=["json"])

    col1, col2 = st.columns(2)
    with col1:
        time_start = st.date_input("Từ ngày", datetime.now() - timedelta(days=1))
    with col2:
        time_end = st.date_input("Đến ngày", datetime.now())

    if st.button("⚙️ Phân tích"):
        if json_file:
            rrd_files = read_json(json_file)
        else:
            rrd_files = read_json("BW_Metro.json")
        start_r = int(datetime.combine(time_start, datetime.min.time()).timestamp())
        stop_r = int(datetime.combine(time_end, datetime.max.time()).timestamp())
        args = [(f[0], start_r, stop_r) for f in rrd_files]

        # Gộp các ring theo miền
        merged_rings = {"MB": [], "MN": [], "MT": []}
        for f in rrd_files:
            ring_name = f[5]
            if ring_name.endswith("MB"):
                merged_rings["MB"].append(f)
            elif ring_name.endswith("MN"):
                merged_rings["MN"].append(f)
            elif ring_name.endswith("MT"):
                merged_rings["MT"].append(f)

        with st.spinner("Đang truy vấn và phân tích dữ liệu..."):
            with multiprocessing.Pool(NUM_PROCESSES) as pool:
                results = pool.map(access_file, args)

            ring_data = {}
            for idx, (rrd_file, data) in enumerate(results):
                if not data:
                    continue
                df = process_rrd_data(data)
                if df.empty:
                    continue
                ring_name = rrd_files[idx][5]
                if ring_name in ring_data:
                    ring_data[ring_name]["traffic_in"] += df["traffic_in"]
                    ring_data[ring_name]["traffic_out"] += df["traffic_out"]
                else:
                    ring_data[ring_name] = df.copy()

            # Summary
            summary = []
            for ring_name, df in ring_data.items():
                burstable = next((f[3] for f in rrd_files if f[5] == ring_name), 0)
                commit = next((f[4] for f in rrd_files if f[5] == ring_name), 0)
                capacity = burstable + commit or 1
                values_in = df["traffic_in"].dropna()
                values_out = df["traffic_out"].dropna()
                p95_in = np.percentile(values_in, 95)
                p95_out = np.percentile(values_out, 95)
                max_in = values_in.max()
                max_out = values_out.max()

                summary.append({
                    "Ring": ring_name,
                    "95% In (Gbps)": round(p95_in, 2),
                    "95% Out (Gbps)": round(p95_out, 2),
                    "Max In (Gbps)": round(max_in, 2),
                    "Max Out (Gbps)": round(max_out, 2),
                    "Capacity (Gbps)": capacity,
                    "Hiệu suất In (%)": round((max_in / capacity) * 100, 1),
                    "Hiệu suất Out (%)": round((max_out / capacity) * 100, 1)
                })

            df_sum = pd.DataFrame(summary)
            st.success("✅ Phân tích hoàn tất")
            st.dataframe(df_sum, use_container_width=True)

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()