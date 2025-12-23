import json
import os
import time
import requests
from datetime import datetime
import sys

# Import scrapers từ file scraper_2.py
# Đảm bảo file scraper_2.py nằm cùng thư mục
from scraper_3 import (
        fetch_kdh_news, fetch_vix_news, fetch_dgc_news, fetch_pow_news, 
        fetch_ree_news, fetch_ocb_news, fetch_kbc_news, fetch_pnj_news, 
        fetch_nvl_news, fetch_vnd_news, fetch_msr_news, fetch_gmd_news, 
        fetch_nvb_news, fetch_frt_news, fetch_nab_news, fetch_vci_news, 
        fetch_hcm_news, fetch_ksv_news, fetch_hag_news, fetch_pdr_news
    )

# --- CẤU HÌNH ---
try:
    BOT_TOKEN = os.environ['BOT_TOKEN']
    CHAT_ID = os.environ['CHAT_ID']
except KeyError:
    print("Lỗi: Không tìm thấy BOT_TOKEN hoặc CHAT_ID.")
    print("Hãy đảm bảo đã set Secrets trong GitHub Actions.")
    sys.exit(1) # Dừng chương trình nếu không có key
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "data_news_3.json")

# --- CẤU HÌNH CHẾ ĐỘ CHẠY ---
FORCE_ALERT_MODE = False   # False = Không ép gửi tin cũ (chỉ gửi tin mới phát sinh)
ENABLE_TELEGRAM = True     # True = Bật gửi tin

# Mapping Mã CK -> Hàm xử lý
STOCK_MAP = {
    "KDH": fetch_kdh_news,
    "VIX": fetch_vix_news,
    "DGC": fetch_dgc_news,
    "POW": fetch_pow_news,
    "REE": fetch_ree_news,
    "OCB": fetch_ocb_news,
    "KBC": fetch_kbc_news,
    "PNJ": fetch_pnj_news,
    "NVL": fetch_nvl_news,
    "VND": fetch_vnd_news,
    "MSR": fetch_msr_news,
    "GMD": fetch_gmd_news,
    "NVB": fetch_nvb_news, # Đã làm (NCB)
    "FRT": fetch_frt_news,
    "NAB": fetch_nab_news,
    "VCI": fetch_vci_news,
    "HCM": fetch_hcm_news,
    "KSV": fetch_ksv_news,
    "HAG": fetch_hag_news, # Đã làm (HAGL)
    "PDR": fetch_pdr_news
}

def load_database():
    if not os.path.exists(DB_FILE): return {}
    try:
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except: return {}

def save_database(data):
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"❌ Lỗi lưu file DB: {e}")

def send_telegram(message):
    if not ENABLE_TELEGRAM: 
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID, 
        "text": message, 
        "parse_mode": "HTML", 
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"   ! Lỗi gửi Tele: {e}")

def format_message(stock_code, item):
    date_info = item.get('date', datetime.now().year)
    return (
        f"🚨 <b>{stock_code} - TIN MỚI!</b>\n"
        f"📅 {date_info}\n"
        f"📝 <b>{item['title']}</b>\n"
        f"🔗 <a href='{item['link']}'>Xem chi tiết</a>\n"
        f"#{stock_code}"
    )

def main():
    print(f"--- 🤖 BOT 2 RUNNING (20 Mã Tiếp Theo) | SEND_TELEGRAM={ENABLE_TELEGRAM} ---")
    db_data = load_database()
    
    is_first_run = len(db_data) == 0
    if is_first_run:
        print("⚠️ Chạy lần đầu: Chỉ lưu dữ liệu nền, KHÔNG gửi tin báo (để tránh spam).")

    total_new = 0

    for stock_code, scraper_func in STOCK_MAP.items():
        print(f"\n🔍 {stock_code}...", end="", flush=True)
        seen_ids = set(db_data.get(stock_code, []))
        
        try:
            # Gọi hàm cào
            new_items = scraper_func(seen_ids)
            
            if new_items:
                print(f" ✅ {len(new_items)} tin mới!", end="")
                if stock_code not in db_data: db_data[stock_code] = []
                
                for item in new_items:
                    # 1. Thêm ID vào bộ nhớ để lần sau không lấy lại
                    db_data[stock_code].append(item['id'])
                    
                    # 2. Gửi tin (Chỉ gửi nếu không phải lần chạy đầu tiên HOẶC chế độ Force bật)
                    if ENABLE_TELEGRAM and ((not is_first_run) or FORCE_ALERT_MODE):
                        print(" -> 📨", end="")
                        send_telegram(format_message(stock_code, item))
                        time.sleep(1) # Nghỉ nhẹ để tránh flood Telegram
                
                # 3. Lưu database ngay lập tức sau mỗi mã
                save_database(db_data)
                total_new += len(new_items)
            else:
                print(" 💤", end="")
                
        except Exception as e:
            print(f" ❌ Lỗi Critical: {e}", end="")
            # Vẫn lưu DB để bảo toàn dữ liệu các mã trước đó
            save_database(db_data)

    print(f"\n\n🏁 HOÀN TẤT VÒNG QUÉT. Tổng cộng {total_new} tin mới.")

if __name__ == "__main__":
    main()