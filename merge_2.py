import pandas as pd
import time
import os
from dotenv import load_dotenv

# 載入 .env 變數
load_dotenv()

# 讀取 .env 內的變數
traffic_log_file = os.getenv("TRAFFIC_LOG_FILE")
proxy_list_file = os.getenv("PROXY_LIST_FILE")
matched_csv_file = os.getenv("MATCHED_CSV_FILE")
connected_ips_file = os.getenv("CONNECTED_IPS_FILE")
source_ips_file = os.getenv("SOURCE_IPS_FILE")

# 確保變數不為 None
if not all([traffic_log_file, proxy_list_file, matched_csv_file, connected_ips_file, source_ips_file]):
    raise ValueError("請確保 .env 檔案中包含所有必要的變數")

# 等待訊息
print("\n🔍 log 分析平台分析中...\n")
time.sleep(1)  # 模擬分析時間

# **🔹 1️⃣ 讀取 FortiGate 流量日誌 CSV**
try:
    traffic_df = pd.read_csv(traffic_log_file, dtype=str)  # 確保所有欄位為字串
    print(f"✅ 原始流量日誌包含 {len(traffic_df):,} 條記錄")
except FileNotFoundError:
    print(f"❌ 錯誤: 找不到流量日誌檔案 `{traffic_log_file}`")
    exit(1)

# **🔹 2️⃣ 讀取惡意 IP 清單 TXT**
try:
    with open(proxy_list_file, "r") as file:
        proxy_ips = {ip.strip() for ip in file.readlines() if ip.strip()}  # 去除空行與多餘空格
    print(f"✅ 惡意 IP 清單包含 {len(proxy_ips):,} 個 IP")
except FileNotFoundError:
    print(f"❌ 錯誤: 找不到惡意 IP 清單檔案 `{proxy_list_file}`")
    exit(1)

start_time = time.time()

# **🔹 3️⃣ 透過 set() 進行快速匹配**
# 過濾 `srcip` 或 `dstip` 在惡意 IP 清單中的流量
matched_traffic = traffic_df[
    (traffic_df["srcip"].isin(proxy_ips)) | (traffic_df["dstip"].isin(proxy_ips))
]

# **🔹 4️⃣ 計算匹配的 IP 記錄數量**
matched_count = len(matched_traffic)

# **🔹 5️⃣ 分析結果顯示**
print("\n✅ 分析結果如下：\n")

if matched_count > 0:
    for _, row in matched_traffic.iterrows():
        ip_address = row["srcip"] if row["srcip"] in proxy_ips else row["dstip"]
        country = row.get("srccountry", "Unknown") if row["srcip"] in proxy_ips else row.get("dstcountry", "Unknown")
        transport = row.get("service", "N/A")
        print(f"🚨 找到匹配的惡意 IP！\n   🔹 IP: {ip_address}  來源: {country}  連線類型: {transport}\n")
else:
    print("✅ 沒有發現與惡意 IP 清單匹配的流量。\n")

# **🔹 6️⃣ 生成「被連線清單」與「連線來源清單（含國家名稱）」**
connected_ips = set(matched_traffic["dstip"].dropna().unique())  # 目的 IP
source_ips = matched_traffic[["srcip", "srccountry", "service"]].dropna().drop_duplicates()  # 來源 IP + 國家名稱

# **🔹 7️⃣ 確保 `connected_ips` 內有數據**
print(f"✅ 被連線的 IP 清單應包含 {len(connected_ips)} 個 IP")
print(f"✅ 來源 IP 清單應包含 {len(source_ips)} 個 IP（含國家名稱）")

# **🔹 8️⃣ 存成 TXT 檔**
with open(connected_ips_file, "w") as f:
    f.write("\n".join(connected_ips))

with open(source_ips_file, "w") as f:
    for _, row in source_ips.iterrows():
        f.write(f"{row['srcip']} ({row['srccountry']}) - {row['service']}\n")

# **🔹 9️⃣ 存入完整 CSV**
matched_traffic.to_csv(matched_csv_file, index=False)

# **🔹 🔟 結果總結**
end_time = time.time()
print("\n📊 分析總結\n")
print("===========================\n")
print(f"🔎 總共檢查了  {len(traffic_df):,}  條流量記錄\n")
print(f"🚨 匹配的惡意 IP 記錄數量:  {matched_count}\n")
print(f"📡 生成的「被連線清單」包含 {len(connected_ips)} 個 IP\n")
print(f"📡 生成的「連線來源清單」包含 {len(source_ips)} 個 IP（含國家名稱）\n")
print(f"📁 分析結果已存入 `{matched_csv_file}`（包含完整的匹配紀錄）\n")
print(f"📄 被連線的 IP 清單已存入 `{connected_ips_file}`\n")
print(f"📄 連線來源的 IP 清單已存入 `{source_ips_file}`\n")
print(f"⏳ 總處理時間: {end_time - start_time:.2f} 秒\n")
print("===========================\n")
