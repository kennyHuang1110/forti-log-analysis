import csv
import re
import os
from datetime import datetime
from dotenv import load_dotenv

# 載入 .env 變數
load_dotenv()

# 讀取 .env 內的變數
input_file_event = os.getenv("INPUT_FILE_EVENT")
output_file_event = os.getenv("OUTPUT_FILE_EVENT")
input_file_traffic = os.getenv("INPUT_FILE_TRAFFIC")
output_file_traffic = os.getenv("OUTPUT_FILE_TRAFFIC")

# 確保變數不為 None
if not all([input_file_event, output_file_event, input_file_traffic, output_file_traffic]):
    raise ValueError("請確保 .env 檔案中包含所有必要的變數")


def convert_log_to_csv(input_file, output_file, log_type):
    """ 解析 FortiCloud 日誌並轉換為 CSV """
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = None  # 初始化 CSV writer
        all_fields = set()  # 存儲所有欄位
        log_entries = []  # 暫存所有日誌條目

        for line in infile:
            # 只處理包含 "date=" 的日誌行
            if not line.strip().startswith("date="):
                continue
            
            # 正則表達式匹配 key=value 格式
            matches = re.findall(r'(\w+)=\"?([^"\s]+)\"?', line)
            if not matches:
                continue

            log_data = dict(matches)

            # 合併 date 和 time 為 timestamp
            date = log_data.get("date", "")
            time = log_data.get("time", "")
            if date and time:
                try:
                    timestamp = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S").isoformat()
                except ValueError:
                    timestamp = ""
            else:
                timestamp = ""

            log_data["timestamp"] = timestamp

            # 移除 date 和 time 欄位
            log_data.pop("date", None)
            log_data.pop("time", None)

            # 更新欄位集合
            all_fields.update(log_data.keys())
            log_entries.append(log_data)

        # 建立 CSV 寫入器，確保所有欄位完整
        all_fields = sorted(all_fields)  # 排序欄位名稱以保持一致
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=["timestamp"] + all_fields)
            writer.writeheader()
            for entry in log_entries:
                row = {field: entry.get(field, '') for field in ["timestamp"] + all_fields}
                writer.writerow(row)

    print(f"✅ {log_type} 日誌轉換完成，輸出至 {output_file}")


# 執行轉換（事件日誌）
convert_log_to_csv(input_file_event, output_file_event, "事件日誌")

# 執行轉換（流量日誌）
convert_log_to_csv(input_file_traffic, output_file_traffic, "流量日誌")
