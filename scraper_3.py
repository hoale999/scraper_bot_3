import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import urllib3
import ssl
import json
import re
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util import ssl_
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC

# Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CẤU HÌNH SSL FIX (Giữ nguyên từ các bot trước) ---
class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl_.create_urllib3_context()
        ctx.options |= 0x4 
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx
        )

def fetch_kdh_news(seen_ids):
    """
    Hàm cào Khang Điền (KDH).
    - URL 1: Báo cáo & Cáo bạch (Lọc lấy BCTC).
    - URL 2: ĐHĐCĐ (Xử lý layout nằm ngang).
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "BCTC & Cáo bạch",
            "url": "https://www.khangdien.com.vn/co-dong/bao-cao-cao-bach",
            "type": "BCTC", # Đánh dấu để lọc từ khóa
            "selector": "li" # Bên BCTC nó nằm trong thẻ li
        },
        {
            "name": "Đại hội đồng cổ đông",
            "url": "https://www.khangdien.com.vn/co-dong/dai-hoi-dong-co-dong",
            "type": "AGM",
            "selector": ".stockcol" # Bên ĐHĐCĐ nó nằm trong div class stockcol (layout ngang)
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    
    # Setup Session
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét KDH (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Chọn các phần tử chứa tin dựa trên config
            items = soup.select(config["selector"])
            
            count_in_page = 0
            
            for item in items:
                # 1. TÌM NGÀY THÁNG (Quan trọng nhất)
                # Dựa trên ảnh: Ngày nằm trong thẻ <i>(29/10/2025)</i>
                date_tag = item.select_one('i')
                if not date_tag: continue
                
                raw_date_text = date_tag.get_text(strip=True)
                
                # Dùng Regex để bắt chuỗi dd/mm/yyyy nằm trong ngoặc đơn
                match = re.search(r'(\d{2}/\d{2}/\d{4})', raw_date_text)
                if not match: continue
                
                date_str = match.group(1)
                
                try:
                    pub_date = datetime.strptime(date_str, "%d/%m/%Y")
                    # LỌC NĂM: Chỉ lấy năm hiện tại
                    if pub_date.year != current_year:
                        continue
                except:
                    continue

                # 2. TÌM LINK & TITLE
                a_tag = item.select_one('a')
                if not a_tag: continue
                
                link = a_tag.get('href')
                title = a_tag.get_text(strip=True) or a_tag.get('title')
                
                if not link or not title: continue
                
                # 3. LỌC TỪ KHÓA (Chỉ áp dụng cho mục BCTC như yêu cầu)
                if config["type"] == "BCTC":
                    title_lower = title.lower()
                    # Chỉ lấy nếu tiêu đề chứa các từ khóa tài chính
                    keywords = ["bctc", "báo cáo tài chính", "financial", "lợi nhuận", "soát xét", "kiểm toán"]
                    if not any(kw in title_lower for kw in keywords):
                        continue

                # 4. CHUẨN HÓA LINK
                if not link.startswith('http'):
                    link = f"https://www.khangdien.com.vn{link}"
                    
                # 5. CHECK TRÙNG
                news_id = link
                if news_id in seen_ids: continue
                if any(x['id'] == news_id for x in new_items): continue

                new_items.append({
                    "source": f"KDH - {config['name']}",
                    "id": news_id,
                    "title": title,
                    "date": date_str,
                    "link": link
                })
                count_in_page += 1
                
            time.sleep(1)

        except Exception as e:
            print(f"[KDH] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

# Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CẤU HÌNH SSL FIX ---
class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl_.create_urllib3_context()
        ctx.options |= 0x4 
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx
        )

def fetch_vix_news(seen_ids):
    """
    Hàm cào Chứng khoán VIX.
    - URL 1 (BCTC): Dùng Tab (#menu2025).
    - URL 2 (ĐHĐCĐ): Dùng Bảng trực tiếp (#tblPublish).
    """
    
    current_year = datetime.now().year
    
    sources = [
        {
            "name": "BCTC",
            "url": "https://vixs.vn/bao-cao",
            "type": "GRID_TAB" # Loại 1: Bảng Grid nằm trong Tab
        },
        {
            "name": "ĐHĐCĐ",
            "url": "https://vixs.vn/qhcd/dai-hoi-co-dong",
            "type": "DIRECT_TABLE" # Loại 2: Bảng trực tiếp, không có Tab năm
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét VIX (Năm {current_year}) ---")

    for source in sources:
        try:
            # print(f"   >> Đang tải: {source['name']}...")
            response = session.get(source["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- XỬ LÝ URL 1: BCTC (GRID trong TAB) ---
            if source["type"] == "GRID_TAB":
                # Tìm Tab năm hiện tại
                year_tab_id = f"menu{current_year}"
                year_content = soup.find(id=year_tab_id)
                
                if not year_content: continue
                
                table = year_content.find('table')
                if not table: continue
                
                # Lấy Header cột
                headers_text = [th.get_text(strip=True) for th in table.select('thead th')]
                
                # Duyệt dòng
                for tr in table.select('tbody tr'):
                    cells = tr.find_all('td')
                    if not cells: continue
                    
                    row_title = cells[0].get_text(strip=True)
                    
                    # Duyệt các ô Quý
                    for i, cell in enumerate(cells[1:], start=1):
                        a_tag = cell.find('a')
                        if not a_tag: continue
                        
                        link = a_tag.get('href')
                        
                        # Lấy ngày ẩn
                        date_div = cell.select_one('.date-pdf')
                        date_str = date_div.get_text(strip=True) if date_div else str(current_year)
                        
                        col_name = headers_text[i] if i < len(headers_text) else f"Quý {i}"
                        full_title = f"{row_title} - {col_name}"
                        
                        if not link: continue
                        if link in seen_ids: continue
                        if any(x['id'] == link for x in new_items): continue

                        new_items.append({
                            "source": f"VIX - {source['name']}",
                            "id": link,
                            "title": full_title,
                            "date": date_str,
                            "link": link
                        })

            # --- XỬ LÝ URL 2: ĐHĐCĐ (BẢNG TRỰC TIẾP) ---
            elif source["type"] == "DIRECT_TABLE":
                # Tìm bảng có id="tblPublish" (Dựa trên ảnh image_1593c0.png)
                table = soup.find(id="tblPublish")
                if not table:
                    # Fallback: Tìm theo class nếu ID đổi
                    table = soup.select_one('.table-report')
                
                if not table: 
                    # print("   -> Không tìm thấy bảng dữ liệu.")
                    continue

                rows = table.select('tbody tr')
                for tr in rows:
                    # 1. TÌM TIÊU ĐỀ & LINK
                    # Class: .bic-report__title a
                    title_div = tr.select_one('.bic-report__title a')
                    if not title_div: continue
                    
                    title = title_div.get_text(strip=True)
                    link = title_div.get('href')
                    
                    # 2. TÌM NGÀY THÁNG
                    # Class: .bic-report__date (VD: 28/11/2025)
                    date_div = tr.select_one('.bic-report__date')
                    if not date_div: continue
                    
                    date_str = date_div.get_text(strip=True)
                    
                    # 3. LỌC NĂM
                    try:
                        pub_date = datetime.strptime(date_str, "%d/%m/%Y")
                        if pub_date.year != current_year:
                            continue
                    except:
                        continue # Lỗi ngày -> Bỏ qua
                        
                    if not link: continue
                    
                    # 4. CHECK TRÙNG
                    if link in seen_ids: continue
                    if any(x['id'] == link for x in new_items): continue

                    new_items.append({
                        "source": f"VIX - {source['name']}",
                        "id": link,
                        "title": title,
                        "date": date_str,
                        "link": link
                    })

            time.sleep(0.5)

        except Exception as e:
            print(f"[VIX] Lỗi tại {source['name']}: {e}")
            continue

    return new_items

# Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CẤU HÌNH SSL FIX ---
class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl_.create_urllib3_context()
        ctx.options |= 0x4 
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx
        )

def fetch_dgc_news(seen_ids):
    """
    Hàm cào Hóa chất Đức Giang (DGC).
    - URL 1: ĐHĐCĐ.
    - URL 2: BCTC (Lọc bỏ bản English).
    - Xử lý ngày tháng dạng: Day="03", Month="2025, Mar".
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Đại hội cổ đông",
            "url": "https://ducgiangchem.vn/category/quan-he-co-dong/dai-hoi-co-dong/",
            "filter_english": False
        },
        {
            "name": "Báo cáo tài chính",
            "url": "https://ducgiangchem.vn/category/quan-he-co-dong/bao-cao-tai-chinh/",
            "filter_english": True # Bật chế độ lọc bản tiếng Anh
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét DGC (Năm {current_year}) ---")

    for config in configs:
        # Quét 2 trang đầu cho chắc (dù thường tin mới ở trang 1)
        # URL phân trang của WordPress: /page/2/
        for page in range(1, 3):
            url = config["url"]
            if page > 1:
                url = f"{config['url']}page/{page}/"
            
            try:
                response = session.get(url, headers=headers, timeout=20, verify=False)
                if response.status_code != 200:
                    # Nếu hết trang (404) thì dừng
                    break
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Tìm các bài viết (article)
                articles = soup.select('article.type-post')
                
                if not articles: break
                
                count_in_page = 0
                for art in articles:
                    # 1. XỬ LÝ NGÀY THÁNG (Phức tạp nhất ở web này)
                    # HTML: <span class="day">03</span> <span class="month">2025, Mar</span>
                    day_tag = art.select_one('.day')
                    month_tag = art.select_one('.month')
                    
                    if not day_tag or not month_tag: continue
                    
                    day_text = day_tag.get_text(strip=True) # "03"
                    month_text = month_tag.get_text(strip=True) # "2025, Mar"
                    
                    # Ghép lại thành chuỗi: "03 2025, Mar"
                    full_date_str = f"{day_text} {month_text}"
                    
                    try:
                        # Parse ngày tháng tiếng Anh (%b là tên tháng viết tắt: Jan, Feb, Mar...)
                        pub_date = datetime.strptime(full_date_str, "%d %Y, %b")
                        
                        if pub_date.year != current_year:
                            continue
                        
                        date_display = pub_date.strftime("%d/%m/%Y")
                    except:
                        continue # Lỗi format ngày -> Bỏ qua

                    # 2. TÌM TIÊU ĐỀ & LINK
                    title_tag = art.select_one('.entry-title a')
                    if not title_tag: continue
                    
                    title = title_tag.get_text(strip=True)
                    link = title_tag.get('href')
                    
                    if not link: continue
                    
                    # 3. LỌC BẢN TIẾNG ANH (Cho mục BCTC)
                    if config["filter_english"]:
                        title_lower = title.lower()
                        # Loại nếu tiêu đề chứa "(english)" hoặc "financial statements"
                        if "(english)" in title_lower or "financial statements" in title_lower:
                            continue
                    
                    # 4. CHECK TRÙNG
                    if link in seen_ids: continue
                    if any(x['id'] == link for x in new_items): continue

                    new_items.append({
                        "source": f"DGC - {config['name']}",
                        "id": link,
                        "title": title,
                        "date": date_display,
                        "link": link
                    })
                    count_in_page += 1
                
                # Nếu trang này không có tin nào của năm nay -> Dừng (vì tin xếp theo thời gian)
                if count_in_page == 0: break
                
                time.sleep(0.5)

            except Exception as e:
                print(f"[DGC] Lỗi tại {config['name']}: {e}")
                break

    return new_items

def fetch_pow_news(seen_ids):
    """
    Hàm cào PV Power (POW).
    - Cấu trúc: Grid layout (col-sm-6/12).
    - Ngày tháng: (dd.mm.yyyy) -> Cần parse dấu chấm.
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Đại hội cổ đông",
            "url": "https://pvpower.vn/vi/tag/dai-hoi-co-dong-23.htm"
        },
        {
            "name": "Báo cáo tài chính",
            "url": "https://pvpower.vn/vi/tag/bao-cao-tai-chinh-10.htm"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    
    # Sử dụng lại session và adapter từ code chính
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét POW (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm tất cả các khối tin (wrapper)
            # Class 'post-item-wrapper' bao quanh cả col-sm-6 và col-sm-12
            items = soup.select('.post-item-wrapper')
            
            count_in_page = 0
            
            for item in items:
                # 1. TÌM NGÀY THÁNG
                # HTML: <span class="published-date">(25.09.2025)</span>
                date_tag = item.select_one('.published-date')
                if not date_tag: continue
                
                raw_date = date_tag.get_text(strip=True).strip('()') # Bỏ ngoặc đơn
                
                try:
                    # Parse định dạng dd.mm.yyyy
                    pub_date = datetime.strptime(raw_date, "%d.%m.%Y")
                    
                    if pub_date.year != current_year:
                        continue
                        
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue # Lỗi ngày -> Bỏ qua

                # 2. TÌM TIÊU ĐỀ & LINK
                # Tiêu đề nằm trong h2 hoặc h3 class="title"
                title_tag = item.select_one('.title a')
                if not title_tag: continue
                
                title = title_tag.get_text(strip=True) or title_tag.get('title')
                link = title_tag.get('href')
                
                if not link: continue
                
                # Chuẩn hóa Link (POW dùng link tương đối)
                if not link.startswith('http'):
                    link = f"https://pvpower.vn{link}"
                
                # 3. CHECK TRÙNG
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": f"POW - {config['name']}",
                    "id": link,
                    "title": title,
                    "date": date_display,
                    "link": link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[POW] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

def fetch_ree_news(seen_ids):
    """
    Hàm cào Cơ điện lạnh (REE).
    - Cấu trúc chuẩn: .vii-report-item
    - Ngày tháng: Lấy từ thuộc tính datetime="YYYY-MM-DD" của thẻ <time>.
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Báo cáo tài chính",
            "url": "https://www.reecorp.com/danh-muc-bao-cao/bao-cao-tai-chinh/"
        },
        {
            "name": "Nghị quyết HĐQT",
            "url": "https://www.reecorp.com/danh-muc-tai-lieu/nghi-quyet-hdqt/"
        },
        {
            "name": "Đại hội cổ đông",
            "url": "https://www.reecorp.com/danh-muc-tai-lieu/dai-hoi-co-dong/"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    
    # Sử dụng lại session và adapter toàn cục
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét REE (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # REE hiển thị dạng list dọc, mỗi item là 1 div class="vii-report-item..."
            # Sử dụng select class bắt đầu bằng vii-report-item
            items = soup.select('.vii-report-item')
            
            count_in_page = 0
            
            for item in items:
                # 1. TÌM NGÀY THÁNG
                # HTML: <time datetime="2025-10-30">30/10/2025</time>
                time_tag = item.select_one('time')
                if not time_tag: continue
                
                # Ưu tiên lấy từ thuộc tính datetime (chuẩn ISO)
                date_iso = time_tag.get('datetime')
                date_text = time_tag.get_text(strip=True)
                
                try:
                    if date_iso:
                        pub_date = datetime.strptime(date_iso, "%Y-%m-%d")
                    else:
                        pub_date = datetime.strptime(date_text, "%d/%m/%Y")
                        
                    if pub_date.year != current_year:
                        continue
                        
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue 

                # 2. TÌM TIÊU ĐỀ
                # HTML: <h3 class="vii-report-item__title">...</h3>
                title_tag = item.select_one('.vii-report-item__title')
                if not title_tag: continue
                title = title_tag.get_text(strip=True)

                # 3. TÌM LINK TẢI
                # HTML: <div class="... download ..."><a href="...">
                download_div = item.select_one('.download a')
                # Fallback: Nếu không có nút download, thử lấy nút ebook hoặc link title
                if not download_div:
                    download_div = item.select_one('.ebook a')
                
                if not download_div: continue
                
                link = download_div.get('href')
                if not link: continue
                
                # 4. CHECK TRÙNG
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": f"REE - {config['name']}",
                    "id": link,
                    "title": title,
                    "date": date_display,
                    "link": link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[REE] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

# Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl_.create_urllib3_context()
        ctx.options |= 0x4 
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx
        )

def fetch_ocb_news(seen_ids):
    """
    Hàm cào OCB (Ngân hàng Phương Đông).
    - Phương pháp: Extract JSON từ thẻ <script id="serverApp-state">.
    - Logic mới: Dựa trên cấu trúc snippet user cung cấp (có key 'fileMedia', 'year').
    """
    
    current_year = datetime.now().year
    
    # URL này chứa cục JSON to đùng
    url = "https://www.ocb.com.vn/vi/nha-dau-tu"
    
    # Base URL để ghép link PDF (Dựa trên domain API trong snippet)
    # Lưu ý: Nếu link 404, có thể thử thêm /reports/ hoặc /documents/ vào sau /uploads/
    base_file_url = "https://webocb-api.ocb.com.vn/uploads/" 
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét OCB (Năm {current_year}) ---")

    try:
        response = session.get(url, headers=headers, timeout=30, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. TÌM THẺ SCRIPT
        script_tag = soup.find('script', id='serverApp-state')
        if not script_tag or not script_tag.string:
            print("[OCB] Không tìm thấy dữ liệu nền.")
            return []
            
        # 2. LOAD JSON
        data_store = json.loads(script_tag.string)
        
        # 3. HÀM ĐỆ QUY TÌM DỮ LIỆU (Scanner)
        # Chúng ta đi tìm mọi dict có chứa key 'fileMedia' và 'year'
        found_docs = []

        def recursive_search(data):
            if isinstance(data, dict):
                # Kiểm tra dấu hiệu nhận biết theo snippet bạn gửi
                if 'fileMedia' in data and 'name' in data:
                    found_docs.append(data)
                
                # Tiếp tục đào sâu vào các key con
                for key, value in data.items():
                    recursive_search(value)
            
            elif isinstance(data, list):
                for item in data:
                    recursive_search(item)

        # Kích hoạt hàm tìm kiếm
        recursive_search(data_store)
        
        # 4. XỬ LÝ DỮ LIỆU TÌM ĐƯỢC
        count_valid = 0
        for item in found_docs:
            # --- LỌC NĂM ---
            # Snippet có sẵn key "year": 2025 (kiểu số int)
            item_year = item.get('year')
            if item_year != current_year:
                continue

            # --- LẤY THÔNG TIN ---
            title = item.get('name')
            file_name = item.get('fileMedia')
            
            if not title or not file_name: continue
            
            # --- XỬ LÝ NGÀY THÁNG ---
            # Snippet: "publishDate": "2025-07-30T00:00:00"
            publish_date = item.get('publishDate')
            date_display = str(current_year)
            
            if publish_date:
                try:
                    # Parse ISO format
                    dt_obj = datetime.fromisoformat(publish_date)
                    date_display = dt_obj.strftime("%d/%m/%Y")
                except:
                    pass

            # --- TẠO LINK HOÀN CHỈNH ---
            full_link = f"{base_file_url}{file_name}"
            
            # --- CHECK TRÙNG ---
            # Dùng tên file làm ID vì nó là duy nhất (có timestamp trong tên file)
            news_id = file_name 
            
            if news_id in seen_ids: continue
            if any(x['id'] == news_id for x in new_items): continue

            new_items.append({
                "source": "OCB - Investor JSON",
                "id": news_id,
                "title": title,
                "date": date_display,
                "link": full_link
            })
            count_valid += 1

    except Exception as e:
        print(f"[OCB] Lỗi xử lý: {e}")

    return new_items

def fetch_kbc_news(seen_ids):
    """
    Hàm cào Kinh Bắc City (KBC).
    - Cấu trúc: div.dk-item
    - Ngày tháng: .dk-item-date (dd/mm/yyyy)
    - Link tải: Ưu tiên link trong nút "Tải về" (.btndl-it)
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Đại hội cổ đông",
            "url": "https://kinhbaccity.vn/dai-hoi-dong-co-dong.htm"
        },
        {
            "name": "Báo cáo tài chính",
            "url": "https://kinhbaccity.vn/bao-cao-tai-chinh.htm"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét KBC (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm danh sách các bài viết
            items = soup.select('.dk-item')
            
            count_in_page = 0
            
            for item in items:
                # 1. TÌM NGÀY THÁNG
                # HTML: <div class="dk-item-date">...</i>28/06/2025</div>
                date_div = item.select_one('.dk-item-date')
                if not date_div: continue
                
                date_text = date_div.get_text(strip=True)
                
                try:
                    pub_date = datetime.strptime(date_text, "%d/%m/%Y")
                    if pub_date.year != current_year:
                        continue
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue # Lỗi ngày -> Bỏ qua

                # 2. TÌM TIÊU ĐỀ
                # HTML: <h3 class="dk-item-title ..."><a ...>Tiêu đề</a></h3>
                title_tag = item.select_one('.dk-item-title a')
                if not title_tag: continue
                title = title_tag.get_text(strip=True)

                # 3. TÌM LINK TẢI
                # Ưu tiên nút "Tải về" (Download link)
                # HTML: <a class="btndl-it ..." href="...">
                download_link = item.select_one('.dk-item-desc .btndl-it')
                
                link = ""
                if download_link:
                    link = download_link.get('href')
                else:
                    # Fallback: Lấy link từ tiêu đề nếu không có nút tải
                    link = title_tag.get('href')
                
                if not link: continue
                
                # Chuẩn hóa Link
                if not link.startswith('http'):
                    # KBC đôi khi dùng link tương đối
                    if link.startswith('/'):
                        link = f"https://kinhbaccity.vn{link}"
                    else:
                        link = f"https://kinhbaccity.vn/{link}"
                
                # 4. CHECK TRÙNG
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": f"KBC - {config['name']}",
                    "id": link,
                    "title": title,
                    "date": date_display,
                    "link": link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[KBC] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

def fetch_pnj_news(seen_ids):
    """
    Hàm cào PNJ (Phiên bản List-Oriented).
    - Ưu tiên tìm thẻ <li> để tách tin (Fix lỗi dính Header chữ đen).
    - Fallback sang cắt <br> nếu không có <li>.
    - Tự động sửa lỗi năm (025 -> 2025).
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Đại hội cổ đông",
            "url": "https://www.pnj.com.vn/quan-he-co-dong/dai-hoi-dong-co-dong/"
        },
        {
            "name": "Báo cáo tài chính",
            "url": "https://www.pnj.com.vn/quan-he-co-dong/bao-cao-tai-chinh/"
        },
        {
            "name": "Nghị quyết HĐQT",
            "url": "https://www.pnj.com.vn/quan-he-co-dong/nghi-quyet-cua-hdqt/"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét PNJ (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 1. TÌM KHỐI DỮ LIỆU NĂM HIỆN TẠI
            # Tìm thẻ h2 chứa "Năm 2025" (hoặc 2025)
            year_header = soup.find('h2', string=re.compile(str(current_year)))
            if not year_header: 
                # print(f"   [PNJ - {config['name']}] Không thấy mục Năm {current_year}")
                continue
                
            # Tìm div nội dung (class="answer")
            question_div = year_header.find_parent(class_='question')
            if not question_div: continue
            answer_div = question_div.find_next_sibling(class_='answer')
            if not answer_div: continue
            
            # 2. XÁC ĐỊNH DANH SÁCH ITEM (Logic quan trọng nhất)
            # Kiểm tra xem có thẻ <li> không (như trong snippet bạn gửi)
            list_items = answer_div.find_all('li')
            
            items_to_process = []
            
            if list_items:
                # [CASE A - ƯU TIÊN] Nếu có <li>: Duyệt từng thẻ li. 
                # Header chữ đen nằm trong thẻ <p> bên ngoài <ol>, nên sẽ TỰ ĐỘNG BỊ LOẠI BỎ.
                items_to_process = list_items
            else:
                # [CASE B - FALLBACK] Nếu không có <li> (dạng văn bản trôi nổi): Cắt chuỗi theo <br>
                raw_html = answer_div.decode_contents()
                lines = re.split(r'<br\s*/?>', raw_html)
                for line in lines:
                    if line.strip():
                        items_to_process.append(BeautifulSoup(line, 'html.parser'))

            # 3. DUYỆT QUA TỪNG ITEM
            count_in_page = 0
            
            for item_soup in items_to_process:
                # --- LỌC RÁC ---
                # Nếu item không có thẻ <a> nào -> Bỏ qua ngay
                all_links = item_soup.find_all('a', href=True)
                if not all_links: 
                    continue

                # --- TÌM LINK TIẾNG VIỆT ---
                target_link = None
                for a_tag in all_links:
                    txt = a_tag.get_text(strip=True).lower()
                    href = a_tag.get('href')
                    # Lấy link không chứa chữ "english" trong text và href
                    if "english" not in txt and "english" not in href.lower():
                        target_link = href
                        break
                
                if not target_link: continue

                # --- LẤY TEXT ĐỂ TÌM NGÀY VÀ TITLE ---
                full_text = item_soup.get_text(" ", strip=True)
                
                # Regex tìm ngày tháng: (dd/mm/yyyy) hoặc (dd/mm/yyy)
                # Chấp nhận năm có 3 hoặc 4 chữ số để bắt lỗi "025"
                match = re.search(r'\((\d{1,2}/\d{1,2}/\d{3,4})\)', full_text)
                
                date_str = ""
                title = ""
                
                if match:
                    raw_date = match.group(1)
                    # Title là phần text TRƯỚC ngày tháng
                    title = full_text[:match.start()].strip(' -:')
                    
                    # Fix lỗi năm (VD: 025 -> 2025)
                    parts = raw_date.split('/')
                    if len(parts) == 3:
                        d, m, y = parts
                        if len(y) == 3 and y.startswith('0'): 
                            y = "2" + y # 025 -> 2025
                        date_str = f"{d}/{m}/{y}"
                else:
                    # Fallback nếu không thấy ngày trong ngoặc
                    title = re.sub(r'(Tải về|Xem).*$', '', full_text).strip(' -:')
                    date_str = str(current_year)

                # --- LỌC NĂM (Final Check) ---
                if str(current_year) not in date_str:
                    continue

                # --- CHUẨN HÓA LINK ---
                if not target_link.startswith('http'):
                    target_link = f"https://www.pnj.com.vn{target_link}"
                if target_link.startswith('//'):
                    target_link = f"https:{target_link}"

                # --- CHECK TRÙNG ---
                if target_link in seen_ids: continue
                if any(x['id'] == target_link for x in new_items): continue

                new_items.append({
                    "source": f"PNJ - {config['name']}",
                    "id": target_link,
                    "title": title,
                    "date": date_str,
                    "link": target_link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[PNJ] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

def fetch_nvl_news(seen_ids):
    """
    Hàm cào Novaland (NVL).
    - Cấu trúc: Table chuẩn.
    - Ngày tháng: Cột 2 (td index 1).
    - Link & Title: Cột 3 (td index 2), lấy từ thẻ <a>.
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Báo cáo tài chính",
            "url": "https://www.novaland.com.vn/quan-he-dau-tu/cong-bo-thong-tin/bao-cao-tai-chinh"
        },
        {
            "name": "Đại hội cổ đông",
            # NVL phân trang ĐHĐCĐ theo năm trên URL
            "url": f"https://www.novaland.com.vn/quan-he-dau-tu/dai-hoi-dong-co-dong/{current_year}"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét NVL (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            
            # Xử lý trường hợp trang năm 2025 chưa tồn tại (redirect về trang chủ hoặc lỗi 404)
            if response.status_code != 200:
                # print(f"   [NVL] Link {config['name']} chưa có dữ liệu hoặc lỗi.")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm bảng dữ liệu (class="table")
            table = soup.select_one('table.table')
            if not table: continue
            
            # Duyệt các dòng trong tbody
            rows = table.select('tbody tr')
            
            count_in_page = 0
            
            for tr in rows:
                cells = tr.find_all('td')
                if len(cells) < 3: continue
                
                # 1. TÌM NGÀY THÁNG (Cột 2)
                date_text = cells[1].get_text(strip=True)
                
                try:
                    pub_date = datetime.strptime(date_text, "%d/%m/%Y")
                    if pub_date.year != current_year:
                        continue
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue # Lỗi ngày -> Bỏ qua

                # 2. TÌM LINK & TITLE (Cột 3)
                link_tag = cells[2].find('a')
                if not link_tag: continue
                
                link = link_tag.get('href')
                # Lấy title từ thuộc tính title của thẻ a (chuẩn nhất theo ảnh)
                title = link_tag.get('title') or link_tag.get_text(strip=True)
                
                if not link: continue
                
                # Chuẩn hóa Link
                if not link.startswith('http'):
                    link = f"https://www.novaland.com.vn{link}"
                
                # 3. CHECK TRÙNG
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": f"NVL - {config['name']}",
                    "id": link,
                    "title": title,
                    "date": date_display,
                    "link": link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[NVL] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

def fetch_vnd_news(seen_ids):
    """
    Hàm cào VNDirect (VND).
    - Link 1, 2: BCTC (Xử lý ngày tháng bị chia nhỏ trong HTML).
    - Link 3: ĐHĐCĐ (Tìm trong sub2congres của năm hiện tại).
    """
    
    current_year = datetime.now().year
    
    # 1. Cấu hình BCTC
    finance_urls = [
        "https://www.vndirect.com.vn/danh_muc_bao_cao/thong-tin-tai-chinh/?key=bao-cao-tai-chinh-hang-nam",
        "https://www.vndirect.com.vn/danh_muc_bao_cao/thong-tin-tai-chinh/?key=bao-cao-tai-chinh-hang-quy"
    ]
    
    # 2. Cấu hình ĐHĐCĐ
    agm_url = "https://www.vndirect.com.vn/dai-hoi-co-dong/"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    
    # Sử dụng lại session và adapter cũ
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét VND (Năm {current_year}) ---")

    # --- PHẦN 1: BÁO CÁO TÀI CHÍNH ---
    for url in finance_urls:
        try:
            response = session.get(url, headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm các khối tin (news-item)
            items = soup.select('.news-item')
            
            for item in items:
                # A. XỬ LÝ NGÀY THÁNG (GHÉP CHUỖI)
                # Cấu trúc: <span class="date-day">14</span>... <p class="date-year">2025</p>
                try:
                    day = item.select_one('.date-day').get_text(strip=True)
                    # Tháng nằm trong thẻ span kế tiếp hoặc sup (tùy format), lấy text của cha chứa nó
                    # Cách an toàn: Lấy text của div 'news-date' rồi dùng regex
                    date_div = item.select_one('.news-date')
                    full_date_text = date_div.get_text(" ", strip=True)
                    
                    # Regex tìm 3 con số: ngày, tháng, năm
                    nums = re.findall(r'\d+', full_date_text)
                    if len(nums) >= 3:
                        d, m, y = nums[0], nums[1], nums[-1] # Năm thường ở cuối hoặc class date-year
                        # Check lại năm từ class date-year cho chắc
                        year_tag = item.select_one('.date-year')
                        if year_tag: y = year_tag.get_text(strip=True)
                        
                        date_str = f"{d}/{m}/{y}"
                        
                        if int(y) != current_year: continue
                    else:
                        continue
                except:
                    continue

                # B. LẤY TIÊU ĐỀ & LINK
                title_tag = item.select_one('h3 a')
                if not title_tag: continue
                
                title = title_tag.get_text(strip=True)
                link = title_tag.get('href')
                
                if not link: continue
                
                # Check trùng
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": "VND - BCTC",
                    "id": link,
                    "title": title,
                    "date": date_str,
                    "link": link
                })

        except Exception as e:
            print(f"[VND-Finance] Lỗi: {e}")

    # --- PHẦN 2: ĐẠI HỘI CỔ ĐÔNG ---
    try:
        response = session.get(agm_url, headers=headers, timeout=20, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm tất cả các Card (Mỗi năm/Sự kiện là 1 card)
        cards = soup.select('.card')
        
        for card in cards:
            # 1. Kiểm tra Header xem có phải Năm hiện tại không
            header = card.select_one('.card-header')
            if not header: continue
            
            header_text = header.get_text(strip=True)
            if str(current_year) not in header_text:
                continue # Bỏ qua các năm cũ
            
            # 2. Tìm vùng "Thông tin chi tiết" (class sub2congres)
            # Lưu ý: sub2congres nằm trong phần collapse
            details_section = card.select_one('.sub2congres')
            if not details_section: continue
            
            # 3. Duyệt các dòng tin trong vùng chi tiết
            infos = details_section.select('.information')
            
            for info in infos:
                # Link & Title
                a_tag = info.select_one('h6 a')
                if not a_tag: continue
                
                title = a_tag.get_text(strip=True)
                link = a_tag.get('href')
                
                # Date: <p class="font13">12:00 11/09/2025</p>
                date_tag = info.select_one('.font13')
                date_display = str(current_year)
                
                if date_tag:
                    raw_date = date_tag.get_text(strip=True)
                    # Regex bắt dd/mm/yyyy
                    match = re.search(r'(\d{2}/\d{2}/\d{4})', raw_date)
                    if match:
                        date_display = match.group(1)
                
                if not link: continue
                
                # Check trùng
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": "VND - ĐHĐCĐ",
                    "id": link,
                    "title": title,
                    "date": date_display,
                    "link": link
                })

    except Exception as e:
        print(f"[VND-AGM] Lỗi: {e}")

    return new_items

def fetch_gmd_news(seen_ids):
    """
    Hàm cào Gemadept (GMD) - Phiên bản Fix Selector.
    - Chiến thuật: Quét class '.wrap-title' (Lõi chứa tin) thay vì container bên ngoài.
    - Đảm bảo lấy đủ cả BCTC và Thông báo.
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Báo cáo tài chính",
            "url": "https://www.gemadept.com.vn/co-dong/bao-cao-tai-chinh/"
        },
        {
            "name": "Thông báo",
            "url": "https://www.gemadept.com.vn/co-dong/thong-bao/"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét GMD (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- THAY ĐỔI SELECTOR ---
            # Thay vì tìm '.list-info-notify', ta tìm thẳng '.wrap-title'
            # Đây là class chứa trực tiếp thẻ <a>, <h5> (Title) và .date
            items = soup.select('.wrap-title')
            
            count_in_page = 0
            
            for item in items:
                # 1. TÌM NGÀY THÁNG
                # HTML: <div class="date ...">21.07.2025</div>
                # Lưu ý: .date nằm bên trong .wrap-title (theo ảnh mới)
                date_div = item.select_one('.date')
                if not date_div: continue
                
                date_text = date_div.get_text(strip=True)
                
                try:
                    # Parse định dạng: 21.07.2025 (dấu chấm)
                    pub_date = datetime.strptime(date_text, "%d.%m.%Y")
                    
                    if pub_date.year != current_year:
                        continue
                        
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue # Lỗi ngày -> Bỏ qua

                # 2. TÌM LINK & TIÊU ĐỀ
                # HTML: <a href="..."><h5>Tiêu đề</h5>...</a>
                # Thẻ a nằm ngay trong .wrap-title hoặc là con trực tiếp
                a_tag = item.find('a')
                if not a_tag: continue
                
                link = a_tag.get('href')
                
                # Lấy tiêu đề: Ưu tiên h5, fallback sang text của a
                h5_tag = a_tag.find('h5')
                if h5_tag:
                    title = h5_tag.get_text(strip=True)
                else:
                    title = a_tag.get_text(strip=True)
                
                if not link: continue
                
                # Chuẩn hóa Link
                if not link.startswith('http'):
                    link = f"https://www.gemadept.com.vn{link}"
                
                # 3. CHECK TRÙNG
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": f"GMD - {config['name']}",
                    "id": link,
                    "title": title,
                    "date": date_display,
                    "link": link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[GMD] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import time

def fetch_nvb_news(seen_ids):
    """
    Hàm chỉ cào BCTC của NCB (Bỏ qua các link bị chặn).
    """
    current_year = datetime.now().year
    
    # Chỉ giữ lại 1 link duy nhất
    target_url = "https://www.ncb-bank.vn/vi/nha-dau-tu/bao-cao-tai-chinh"
    
    # Cấu hình Selenium
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") # Chạy ngầm cho gọn
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    new_items = []
    
    print(f"--- 🚀 Quét NCB (Chỉ BCTC) - Năm {current_year} ---")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        # print(f"   >> Đang truy cập: {target_url}...")
        driver.get(target_url)
        
        # 1. Chờ dữ liệu load (Chờ thẻ h6 class new-download xuất hiện)
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "new-download"))
            )
            time.sleep(2) # Chờ thêm chút cho chắc
        except:
            print("      ⚠️ Timeout: Không thấy dữ liệu BCTC.")
            return []

        # 2. Lấy HTML & Parse
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        items = soup.find_all('h6', class_='new-download')
        
        count_found = 0
        for item in items:
            # Lấy Link & Title
            a_tag = item.find('a')
            if not a_tag: continue
            
            link = a_tag.get('href')
            title = a_tag.get('title') or a_tag.get_text(strip=True)
            
            # Lấy Ngày (thẻ p ngay bên cạnh)
            p_tag = item.find('p')
            date_str = str(current_year)
            
            if p_tag:
                raw_date = p_tag.get_text(strip=True) # VD: 28/10/2025 09:49:00
                try:
                    # Parse ngày
                    clean_date = raw_date.strip()[:10] # Lấy 10 ký tự đầu (dd/mm/yyyy)
                    dt = datetime.strptime(clean_date, "%d/%m/%Y")
                    
                    if dt.year != current_year:
                        continue # Bỏ qua năm cũ
                    date_str = dt.strftime("%d/%m/%Y")
                except:
                    # Nếu lỗi parse nhưng có text năm hiện tại thì vẫn lấy (fallback)
                    if str(current_year) not in raw_date:
                        continue

            # --- LỌC TIẾNG VIỆT (QUAN TRỌNG) ---
            # Bỏ qua các file tiếng Anh
            keywords_en = ["financial report", "statement", "separate", "consolidated", "explanation"]
            if any(kw in title.lower() for kw in keywords_en): 
                continue

            # Chuẩn hóa Link
            if link and not link.startswith('http'):
                link = f"https://www.ncb-bank.vn{link}"
            
            # Check trùng & Lưu
            if link not in seen_ids:
                if not any(x['id'] == link for x in new_items):
                    new_items.append({
                        "source": "NCB - BCTC",
                        "id": link,
                        "title": title,
                        "date": date_str,
                        "link": link
                    })
                    count_found += 1
        
        # print(f"      -> Tìm thấy {count_found} báo cáo mới.")

    except Exception as e:
        print(f"      ❌ Lỗi NCB: {e}")

    finally:
        driver.quit()

    return new_items

def fetch_frt_news(seen_ids):
    """
    Hàm cào FPT Retail (FRT) - Phiên bản Selenium API.
    - Dùng trình duyệt thật để mở link API -> Bypass 403 TLS Fingerprint.
    - Lấy nội dung text từ body trình duyệt (chính là chuỗi JSON) để parse.
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Báo cáo tài chính", 
            "url": "https://api.frt.vn/common/frt-new/api/v1/reports?categoryId=56&locale=vi&page=1&pageSize=10"
        },
        {
            "name": "Công bố thông tin", 
            "url": "https://api.frt.vn/common/frt-new/api/v1/reports?categoryId=54&locale=vi&page=1&pageSize=10"
        },
        {
            "name": "Đại hội cổ đông", 
            "url": "https://api.frt.vn/common/frt-new/api/v1/reports?categoryId=58&locale=vi&page=1&pageSize=10"
        }
    ]

    new_items = []

    # --- CẤU HÌNH SELENIUM ---
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Fake User-Agent xịn
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    print(f"--- 🚀 Bắt đầu quét FRT (Selenium Mode - Năm {current_year}) ---")
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    except Exception as e:
        print(f"[FRT] Lỗi driver: {e}")
        return []

    try:
        for config in configs:
            try:
                # Mở link API bằng trình duyệt
                driver.get(config["url"])
                
                # Lấy toàn bộ text trong thẻ body (Chính là chuỗi JSON thô)
                json_text = driver.find_element(By.TAG_NAME, "body").text
                
                # Parse chuỗi thành Dictionary
                try:
                    json_data = json.loads(json_text)
                except:
                    # print(f"   [FRT] Không phải JSON hợp lệ tại {config['name']}")
                    continue

                # --- XỬ LÝ DỮ LIỆU (Logic cũ) ---
                results = json_data.get('data', {}).get('results', [])
                
                if not results: continue
                
                count_in_cat = 0
                for item in results:
                    attrs = item.get('attributes', {})
                    title = attrs.get('name')
                    
                    # 1. XỬ LÝ NGÀY THÁNG
                    date_iso = attrs.get('updatedAt') or attrs.get('createdAt')
                    date_str = str(current_year)
                    
                    if date_iso:
                        try:
                            date_part = date_iso[:10]
                            pub_date = datetime.strptime(date_part, "%Y-%m-%d")
                            if pub_date.year != current_year:
                                continue
                            date_str = pub_date.strftime("%d/%m/%Y")
                        except: pass

                    # 2. LẤY FILE PDF
                    file_data = attrs.get('file', {}).get('data')
                    if not file_data: continue
                        
                    file_attrs = file_data.get('attributes', {})
                    link = file_attrs.get('url')
                    file_name = file_attrs.get('name')
                    
                    if not link: continue
                    
                    # 3. CHUẨN HÓA LINK
                    if not link.startswith('http'):
                        link = f"https://cdn.frt.vn{link}" if not link.startswith('//') else f"https:{link}"

                    # 4. CHECK TRÙNG
                    news_id = file_name if file_name else link
                    
                    if news_id in seen_ids: continue
                    if any(x['id'] == news_id for x in new_items): continue

                    new_items.append({
                        "source": f"FRT - {config['name']}",
                        "id": news_id,
                        "title": title,
                        "date": date_str,
                        "link": link
                    })
                    count_in_cat += 1
                
                # print(f"   > {config['name']}: Lấy được {count_in_cat} tin.")
                time.sleep(1)

            except Exception as e:
                print(f"[FRT] Lỗi xử lý {config['name']}: {e}")
                continue
    finally:
        driver.quit()

    return new_items

def fetch_nab_news(seen_ids):
    """
    Hàm cào Nam A Bank (NAB) - Phiên bản Stealth Mode.
    - Che giấu dấu hiệu Bot của Selenium để vượt qua WAF.
    - Logic tìm tin: Quét trong .main-list.
    - Logic ngày tháng: Xử lý linh hoạt (trong ngoặc vuông hoặc trong tiêu đề).
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Báo cáo & Công bố",
            "url": "https://www.namabank.com.vn/2025-1"
        },
        {
            "name": "Đại hội cổ đông",
            "url": "https://www.namabank.com.vn/2025-3"
        }
    ]

    new_items = []

    # --- CẤU HÌNH SELENIUM CHE DẤU VẾT ---
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    # Tắt dòng thông báo "Chrome is being controlled..."
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    # Fake User-Agent xịn
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    print(f"--- 🚀 Bắt đầu quét NAB (Stealth Selenium - Năm {current_year}) ---")
    
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        # Hack thêm để che giấu thuộc tính webdriver
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except Exception as e:
        print(f"[NAB] Lỗi driver: {e}")
        return []

    try:
        for config in configs:
            try:
                driver.get(config["url"])
                time.sleep(3) # Chờ load JS
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Tìm vùng chứa danh sách tin
                main_list = soup.select_one('.main-list')
                if not main_list:
                    # print(f"   [NAB] Không thấy .main-list tại {config['name']}")
                    continue
                
                # Tìm các item bài viết (thường là col-md-6 item hoặc col-md-6)
                items = main_list.select('.item')
                if not items:
                    # Fallback tìm class col-md-6 nếu class item bị thiếu
                    items = main_list.select('.col-md-6')

                count_in_page = 0
                for item in items:
                    # Tìm link trong figcaption hoặc icon
                    a_tag = item.select_one('.figcaption a') or item.select_one('.icon a') or item.find('a')
                    if not a_tag: continue
                    
                    link = a_tag.get('href')
                    # Title: Ưu tiên attribute title > text của a
                    raw_title = a_tag.get('title') or a_tag.get_text(strip=True)
                    
                    if not link or not raw_title: continue

                    # --- XỬ LÝ DỮ LIỆU ---
                    clean_title = raw_title
                    date_str = ""
                    
                    # Case 1: Có ngày trong ngoặc vuông [Đăng ngày 29/03/2025]...
                    bracket_match = re.search(r'\[.*(\d{1,2}/\d{1,2}/\d{4}).*\]', raw_title)
                    
                    if bracket_match:
                        raw_date = bracket_match.group(1) # Lấy phần ngày
                        try:
                            pub_date = datetime.strptime(raw_date, "%d/%m/%Y")
                            if pub_date.year != current_year:
                                continue # Bỏ qua năm cũ
                            date_str = raw_date
                        except: pass
                        
                        # Xóa phần ngoặc vuông khỏi tiêu đề
                        clean_title = re.sub(r'\[.*?\]', '', raw_title).strip()
                    
                    # Case 2: Không có ngoặc vuông (thường là BCTC), check năm trong Title
                    else:
                        if str(current_year) in raw_title:
                            date_str = str(current_year)
                        else:
                            # Nếu URL cũng không chứa năm hiện tại thì bỏ qua (vì link web là 2025-x nên khá an toàn)
                            if str(current_year) not in config['url']:
                                continue
                            date_str = str(current_year) # Fallback lấy theo URL

                    # --- [MỚI] LỌC TIẾNG ANH ---
                    # Kiểm tra tiêu đề có chứa từ khóa tiếng Anh không
                    title_upper = clean_title.upper()
                    if "TIẾNG ANH" in title_upper or "ENGLISH" in title_upper:
                        continue

                    # --- CHUẨN HÓA LINK ---
                    if not link.startswith('http'):
                        link = f"https://www.namabank.com.vn{link}"
                    
                    # --- CHECK TRÙNG ---
                    if link in seen_ids: continue
                    if any(x['id'] == link for x in new_items): continue

                    new_items.append({
                        "source": f"NAB - {config['name']}",
                        "id": link,
                        "title": clean_title,
                        "date": date_str,
                        "link": link
                    })
                    count_in_page += 1
                
                # print(f"   > {config['name']}: Lấy được {count_in_page} tin.")

            except Exception as e:
                print(f"[NAB] Lỗi xử lý {config['name']}: {e}")
                continue
    finally:
        if driver: driver.quit()

    return new_items

def fetch_vci_news(seen_ids):
    """
    Hàm cào Vietcap (VCI).
    - Cấu trúc: Thẻ <a> class="listing-item".
    - Web tĩnh (Astro), tốc độ phản hồi rất nhanh.
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Thông tin cổ đông",
            "url": "https://www.vietcap.com.vn/quan-he-co-dong/thong-tin-co-dong"
        },
        {
            "name": "Báo cáo tài chính",
            "url": "https://www.vietcap.com.vn/quan-he-co-dong/bao-cao-tai-chinh"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét VCI (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm danh sách các bài viết (thẻ a có class listing-item)
            items = soup.select('a.listing-item')
            
            count_in_page = 0
            
            for item in items:
                # 1. TÌM NGÀY THÁNG
                # HTML: <div class="date-desktop ...">07/11/2025</div>
                date_div = item.select_one('.date-desktop')
                if not date_div: continue
                
                date_text = date_div.get_text(strip=True)
                
                try:
                    pub_date = datetime.strptime(date_text, "%d/%m/%Y")
                    if pub_date.year != current_year:
                        continue
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue 

                # 2. TÌM LINK & TIÊU ĐỀ
                link = item.get('href')
                
                # Title nằm trong span class="title"
                title_span = item.select_one('.title')
                if not title_span: continue
                
                # Ưu tiên lấy từ attribute 'title' của span để được text đầy đủ (tránh bị cắt dòng)
                title = title_span.get('title') or title_span.get_text(strip=True)
                
                if not link: continue
                
                # Chuẩn hóa Link (VCI dùng link tương đối)
                if not link.startswith('http'):
                    link = f"https://www.vietcap.com.vn{link}"
                
                # 3. CHECK TRÙNG
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": f"VCI - {config['name']}",
                    "id": link,
                    "title": title,
                    "date": date_display,
                    "link": link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[VCI] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

def fetch_hcm_news(seen_ids):
    """
    Hàm cào Chứng khoán HSC (HCM) - Phiên bản Fix BCTC.
    - BCTC: Tìm thẻ <a> chứa class 'text-body2-mobile' (Ngày) và 'text-heading2-mobile' (Tiêu đề).
    - ĐHĐCĐ: Tìm theo Accordion năm hiện tại (Giữ nguyên logic cũ vì đã chạy tốt).
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Báo cáo tài chính",
            "url": "https://www.hsc.com.vn/vi/tai-chinh/bao-cao-tai-chinh",
            "type": "FINANCE"
        },
        {
            "name": "Đại hội cổ đông",
            "url": "https://www.hsc.com.vn/vi/dai-hoi-co-dong",
            "type": "AGM"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét HSC (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- XỬ LÝ 1: BÁO CÁO TÀI CHÍNH (LOGIC MỚI) ---
            if config["type"] == "FINANCE":
                # Tìm tất cả thẻ <a> có href
                all_links = soup.find_all('a', href=True)
                
                for link_tag in all_links:
                    # Kiểm tra xem trong thẻ a này có chứa class ngày và tiêu đề không
                    # Lưu ý: Class của Tailwind rất dài, ta chỉ check từ khóa đặc trưng
                    date_elem = link_tag.find(class_=lambda x: x and 'text-body2-mobile' in x)
                    title_elem = link_tag.find(class_=lambda x: x and 'text-heading2-mobile' in x)
                    
                    if not date_elem or not title_elem:
                        continue
                        
                    # 1. Parse Ngày (text-body2-mobile)
                    date_text = date_elem.get_text(strip=True) # VD: 18.04.2025
                    try:
                        pub_date = datetime.strptime(date_text, "%d.%m.%Y")
                        if pub_date.year != current_year:
                            continue
                        date_display = pub_date.strftime("%d/%m/%Y")
                    except:
                        continue # Lỗi ngày -> Bỏ qua

                    # 2. Lấy Tiêu đề & Link
                    title = title_elem.get_text(strip=True)
                    link = link_tag.get('href')
                    
                    # 3. Check Trùng & Lưu
                    if link in seen_ids: continue
                    if any(x['id'] == link for x in new_items): continue

                    new_items.append({
                        "source": f"HSC - {config['name']}",
                        "id": link,
                        "title": title,
                        "date": date_display,
                        "link": link
                    })

            # --- XỬ LÝ 2: ĐẠI HỘI CỔ ĐÔNG (LOGIC CŨ - ĐÃ TỐT) ---
            elif config["type"] == "AGM":
                # Tìm Header "Đại hội cổ đông thường niên năm 2025"
                year_keyword = f"Đại hội cổ đông thường niên năm {current_year}"
                header = soup.find(string=lambda x: x and year_keyword in x)
                
                if not header: continue
                
                header_elem = header.parent
                # Tìm khối bao quanh (Accordion Item)
                accordion_item = header_elem.find_parent(class_=lambda x: x and ("border-b" in x or "flex-col" in x))
                
                if not accordion_item: continue
                
                # Tìm tất cả link trong khối đó
                # Lọc kỹ hơn: Chỉ lấy link có href chứa file hoặc googleapis
                all_links = accordion_item.find_all('a', href=True)
                
                for a_tag in all_links:
                    link = a_tag.get('href')
                    title = a_tag.get_text(strip=True)
                    
                    # Lấy text sibling nếu title trong thẻ a quá ngắn (icon)
                    if len(title) < 5:
                        sibling = a_tag.find_previous_sibling() or a_tag.parent.find_previous_sibling()
                        if sibling: title = sibling.get_text(strip=True)
                    
                    # Lọc rác
                    if "mailto:" in link or "tel:" in link: continue
                    valid_ext = ('.pdf', '.doc', '.docx', 'googleapis.com')
                    if not any(ext in link.lower() for ext in valid_ext): continue

                    if link in seen_ids: continue
                    if any(x['id'] == link for x in new_items): continue

                    new_items.append({
                        "source": f"HSC - {config['name']}",
                        "id": link,
                        "title": title,
                        "date": str(current_year),
                        "link": link
                    })

            time.sleep(0.5)

        except Exception as e:
            print(f"[HSC] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

def fetch_ksv_news(seen_ids):
    """
    Hàm cào Vimico (KSV).
    - Cấu trúc: div.post.clearfix -> h2.title a
    - Đặc điểm: Không có ngày tháng bên ngoài -> Lấy top tin mới nhất + Lọc theo năm trong Title.
    """
    
    current_year = datetime.now().year
    url = "https://vimico.vn/cong-bo-thong-tin/"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét KSV (Năm {current_year}) ---")

    try:
        response = session.get(url, headers=headers, timeout=20, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm danh sách các bài viết
        items = soup.select('.post.clearfix')
        
        count_in_page = 0
        
        for item in items:
            # 1. TÌM LINK & TIÊU ĐỀ
            # HTML: <h2 class="title"><a href="...">Tiêu đề...</a></h2>
            title_tag = item.select_one('h2.title a')
            if not title_tag: continue
            
            title = title_tag.get_text(strip=True)
            link = title_tag.get('href')
            
            if not link: continue
            
            # 2. XỬ LÝ NGÀY THÁNG (Giả lập)
            # Vì web không hiện ngày, ta dùng chiến thuật:
            # - Nếu Title chứa "2025" -> Lấy chắc chắn.
            # - Nếu không, chỉ lấy nếu nó nằm trong Top 5 tin đầu tiên (giả định là tin mới).
            
            date_str = str(current_year) # Mặc định năm nay
            
            is_relevant = False
            if str(current_year) in title:
                is_relevant = True
            elif count_in_page < 5: # Lấy 5 tin đầu tiên dù không có năm để tránh sót
                is_relevant = True
            
            if not is_relevant: continue

            # Chuẩn hóa Link
            if not link.startswith('http'):
                link = f"https://vimico.vn{link}"
            
            # 3. CHECK TRÙNG
            if link in seen_ids: continue
            if any(x['id'] == link for x in new_items): continue

            new_items.append({
                "source": "KSV - Công bố thông tin",
                "id": link,
                "title": title,
                "date": date_str,
                "link": link
            })
            count_in_page += 1
            
            # Chỉ lấy tối đa 10 tin để tránh spam tin cũ
            if count_in_page >= 10: break

    except Exception as e:
        print(f"[KSV] Lỗi kết nối: {e}")

    return new_items

def fetch_hag_news(seen_ids):
    """
    Hàm cào HAGL (Phiên bản Chuẩn 4 Cột).
    - Xử lý giao diện 2025: Bảng danh sách 4 cột (Nội dung | Năm | Danh mục | Link/Ngày).
    - Vẫn giữ fallback cho dạng Grid cũ nếu có.
    """
    
    current_year = datetime.now().year
    url = "https://www.hagl.com.vn/co-dong"
    
    # Các section cần quét
    target_sections = [
        {"id": "section-table-2", "name": "Báo cáo tài chính"},
        {"id": "section-table-4", "name": "Nghị quyết HĐQT"},
        {"id": "section-table-5", "name": "Đại hội cổ đông"}
    ]

    new_items = []

    # --- CẤU HÌNH SELENIUM ---
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    
    print(f"--- 🚀 Bắt đầu quét HAG (Năm {current_year}) ---")
    
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.set_page_load_timeout(60)
        
        driver.get(url)
        # Chờ bảng load (quan trọng)
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "section-table-2"))
            )
        except:
            print("[HAG] Timeout chờ bảng dữ liệu.")

        # Cuộn trang dần để kích hoạt lazy load
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        for section_config in target_sections:
            sec_id = section_config["id"]
            sec_name = section_config["name"]
            
            section_node = soup.find(id=sec_id)
            if not section_node: continue
            
            table = section_node.find('table')
            if not table: continue
            
            # Lấy header để check dạng Grid (nếu cần)
            headers = table.select('thead th')
            is_grid = len(headers) > 4 # Nếu > 4 cột thường là dạng Grid (Quý 1,2,3,4)
            
            rows = table.select('tbody tr')
            
            for tr in rows:
                cells = tr.find_all('td')
                if not cells: continue
                
                # --- CHIẾN THUẬT 1: DẠNG DANH SÁCH 4 CỘT (Layout 2025) ---
                # Cấu trúc: [Title] [Year] [Category] [Link + Date]
                if len(cells) == 4:
                    # 1. Check Cột Năm (Cột 2 - index 1)
                    year_text = cells[1].get_text(strip=True)
                    if str(current_year) not in year_text:
                        continue 

                    # 2. Lấy Title (Cột 1 - index 0)
                    title = cells[0].get_text(strip=True)
                    
                    # 3. Lấy Link & Date (Cột 4 - index 3)
                    last_cell = cells[3]
                    
                    # Link
                    a_tag = last_cell.find('a')
                    if not a_tag: continue
                    link = a_tag.get('href')
                    
                    # Date (trong badge)
                    date_str = str(current_year)
                    badge = last_cell.select_one('.badge')
                    if badge:
                        raw_date = badge.get_text(strip=True) # VD: 11/11/2025
                        # Regex bắt ngày
                        match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', raw_date)
                        if match:
                            date_str = match.group(1)
                    
                    if not link.startswith('http'): link = f"https://www.hagl.com.vn{link}"
                    
                    if link in seen_ids: continue
                    if any(x['id'] == link for x in new_items): continue

                    new_items.append({
                        "source": f"HAG - {sec_name}",
                        "id": link,
                        "title": title,
                        "date": date_str,
                        "link": link
                    })

                # --- CHIẾN THUẬT 2: DẠNG GRID (Cũ/Fallback) ---
                elif len(cells) > 4: 
                    row_title = cells[0].get_text(strip=True)
                    # Duyệt các ô Quý
                    for i, cell in enumerate(cells[1:], start=1):
                        a_tag = cell.find('a')
                        if not a_tag: continue
                        
                        link = a_tag.get('href')
                        
                        # Date badge
                        badge = cell.select_one('.badge')
                        if not badge: continue
                        raw_date = badge.get_text(strip=True)
                        
                        try:
                            pub_date = datetime.strptime(raw_date, "%d/%m/%Y")
                            if pub_date.year != current_year: continue
                            date_str = raw_date
                        except: continue
                        
                        # Ghép tiêu đề
                        col_name = headers[i].get_text(strip=True) if i < len(headers) else f"Cột {i}"
                        full_title = f"{row_title} - {col_name}"
                        
                        if not link.startswith('http'): link = f"https://www.hagl.com.vn{link}"
                        if link in seen_ids: continue
                        
                        new_items.append({
                            "source": f"HAG - {sec_name}",
                            "id": link,
                            "title": full_title,
                            "date": date_str,
                            "link": link
                        })

    except Exception as e:
        print(f"[HAG] Lỗi xử lý: {e}")
    finally:
        if driver: driver.quit()

    return new_items

def fetch_pdr_news(seen_ids):
    """
    Hàm cào Phát Đạt (PDR).
    - Cấu trúc: .block-record (Mỗi dòng tin là 1 block-record).
    - Ngày tháng: Tìm text sau chữ "Ngày ban hành".
    - Link: Thẻ <a> trong .block-cell.
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Thông báo cổ đông",
            "url": "https://www.phatdat.com.vn/thong-bao-co-dong/"
        },
        {
            "name": "Báo cáo tài chính",
            "url": "https://www.phatdat.com.vn/bao-cao-tai-chinh/"
        },
        {
            "name": "Tài liệu cổ đông",
            "url": "https://www.phatdat.com.vn/tai-lieu-co-dong/"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét PDR (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm danh sách các dòng tin
            # PDR dùng thẻ span class="block-record" cho mỗi dòng
            records = soup.select('.block-record')
            
            count_in_page = 0
            
            for record in records:
                # 1. TÌM NGÀY THÁNG
                # HTML: <span class="block-cell ..."><strong>Ngày ban hành</strong> 27/06/2025</span>
                date_tag = record.find('strong', string=re.compile("Ngày ban hành"))
                
                if not date_tag: continue
                
                # Lấy text của thẻ cha chứa nó
                full_date_text = date_tag.parent.get_text(strip=True)
                # Xóa chữ "Ngày ban hành" để lấy ngày
                date_text = full_date_text.replace("Ngày ban hành", "").strip()
                
                try:
                    pub_date = datetime.strptime(date_text, "%d/%m/%Y")
                    if pub_date.year != current_year:
                        continue
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue # Lỗi ngày hoặc dòng header -> Bỏ qua

                # 2. TÌM LINK & TIÊU ĐỀ
                # Tìm thẻ a trong block-cell (Loại trừ nút download chỉ có icon)
                # Tìm thẻ a có text dài (Tiêu đề)
                a_tags = record.select('a')
                
                target_link = None
                target_title = ""
                
                for a in a_tags:
                    txt = a.get_text(strip=True)
                    # Nếu text dài > 5 ký tự -> Đây là tiêu đề
                    if len(txt) > 5:
                        target_link = a.get('href')
                        target_title = txt
                        break
                
                if not target_link:
                    # Fallback: Lấy thẻ a đầu tiên nếu không lọc được text
                    if a_tags:
                        target_link = a_tags[0].get('href')
                        target_title = a_tags[0].get_text(strip=True) or "Tài liệu PDR"

                if not target_link: continue
                
                # Chuẩn hóa Link
                if not target_link.startswith('http'):
                    target_link = f"https://www.phatdat.com.vn{target_link}"
                
                # 3. CHECK TRÙNG
                if target_link in seen_ids: continue
                if any(x['id'] == target_link for x in new_items): continue

                new_items.append({
                    "source": f"PDR - {config['name']}",
                    "id": target_link,
                    "title": target_title,
                    "date": date_display,
                    "link": target_link
                })
                count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[PDR] Lỗi tại {config['name']}: {e}")
            continue

    return new_items

def fetch_hag_news(seen_ids):
    """
    Hàm cào HAGL (HAG) - Fix lỗi tìm sai thẻ chứa bảng.
    """
    current_year = str(datetime.now().year)
    # current_year = "2025" # Test cứng nếu cần
    
    url = "https://www.hagl.com.vn/co-dong"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét HAGL (Năm {current_year}) ---")

    try:
        response = session.get(url, headers=headers, timeout=20, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Danh sách các mục cần quét và ID của tiêu đề tương ứng
        # section-table-2: Báo cáo tài chính
        # section-table-5: Đại hội đồng cổ đông
        targets = [
            {"name": "BCTC", "header_id": "section-table-2"},
            {"name": "ĐHĐCĐ", "header_id": "section-table-5"}
        ]

        for target in targets:
            header = soup.find(id=target["header_id"])
            if not header:
                continue

            # --- LOGIC MỚI: TÌM BẢNG KẾ TIẾP ---
            # Từ tiêu đề h3, tìm thẻ table xuất hiện tiếp theo trong HTML
            table = header.find_next("table")
            if not table:
                continue

            rows = table.find_all('tr')
            for row in rows:
                # Lấy toàn bộ text trong dòng để kiểm tra năm
                row_text = row.get_text()
                
                # Kiểm tra năm (2025)
                if current_year not in row_text:
                    continue
                
                # Tìm link
                a_tag = row.find('a', href=True)
                if not a_tag: continue
                
                link = a_tag.get('href')
                if not link.startswith('http'):
                    link = f"https://www.hagl.com.vn{link}"
                
                # Lấy tiêu đề từ cột đầu tiên (hoặc text của link)
                cols = row.find_all('td')
                if cols:
                    raw_title = cols[0].get_text(strip=True)
                else:
                    raw_title = a_tag.get_text(strip=True) or target["name"]

                # Lấy ngày (cố gắng tìm trong cột cuối hoặc badge)
                date_str = current_year
                try:
                    badge = row.find('span', class_='badge')
                    if badge:
                        date_text = badge.get_text(strip=True)
                        if '/' in date_text:
                            date_str = date_text[:10] # 24/08/2025
                except:
                    pass

                # Check trùng
                if link in seen_ids: continue
                if any(x['id'] == link for x in new_items): continue

                new_items.append({
                    "source": f"HAGL - {target['name']}",
                    "id": link,
                    "title": raw_title,
                    "date": date_str,
                    "link": link
                })

    except Exception as e:
        print(f"[HAGL] Lỗi: {e}")

    return new_items

def fetch_msr_news(seen_ids):
    """
    Hàm cào Masan High-Tech Materials (MSR).
    - Cấu trúc: .releases-box chứa Date và Content.
    - Xử lý đặc biệt: Một tin có thể có nhiều file đính kèm (trong thẻ <ol> <li>).
    """
    
    current_year = datetime.now().year
    
    configs = [
        {
            "name": "Thông tin tài chính",
            "url": "https://masanhightechmaterials.com/vi/investor_category/thong-tin-tai-chinh/"
        },
        {
            "name": "Thông báo công ty",
            "url": "https://masanhightechmaterials.com/vi/investor_category/thong-bao-cong-ty/"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét MSR (Năm {current_year}) ---")

    for config in configs:
        try:
            response = session.get(config["url"], headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm các khối tin (releases-box)
            boxes = soup.select('.releases-box')
            
            count_in_page = 0
            
            for box in boxes:
                # 1. TÌM NGÀY THÁNG
                # HTML: <div class="date">... 28/10/2025</div>
                date_div = box.select_one('.date')
                if not date_div: continue
                
                date_text = date_div.get_text(strip=True)
                
                try:
                    pub_date = datetime.strptime(date_text, "%d/%m/%Y")
                    if pub_date.year != current_year:
                        continue
                    date_display = pub_date.strftime("%d/%m/%Y")
                except:
                    continue # Lỗi ngày -> Bỏ qua

                # 2. XỬ LÝ LINK & TIÊU ĐỀ
                # MSR có 2 dạng:
                # Dạng A: Link nằm ngay tiêu đề H4
                # Dạng B: Tiêu đề H4 không có link (hoặc link rỗng), bên dưới có list <ol> <li> chứa các file
                
                # Tìm tiêu đề chính
                h4_tag = box.select_one('h4 a')
                main_title = ""
                if h4_tag:
                    main_title = h4_tag.get_text(strip=True)
                    main_link = h4_tag.get('href')
                    
                    # Nếu tiêu đề chính có link hợp lệ -> Lấy luôn
                    if main_link and len(main_link) > 5 and "javascript" not in main_link:
                        if main_link not in seen_ids and not any(x['id'] == main_link for x in new_items):
                            new_items.append({
                                "source": f"MSR - {config['name']}",
                                "id": main_link,
                                "title": main_title,
                                "date": date_display,
                                "link": main_link
                            })
                            count_in_page += 1

                # Tìm các file đính kèm (nếu có)
                sub_links = box.select('ol li a')
                for sub_a in sub_links:
                    sub_href = sub_a.get('href')
                    sub_title = sub_a.get_text(strip=True)
                    
                    if not sub_href: continue
                    
                    # Ghép tiêu đề: Tiêu đề chính + Tiêu đề phụ (để rõ nghĩa)
                    full_title = f"{main_title}: {sub_title}" if main_title else sub_title
                    
                    # Chuẩn hóa link
                    if not sub_href.startswith('http'):
                        sub_href = f"https://masanhightechmaterials.com{sub_href}"
                        
                    # Check trùng
                    if sub_href in seen_ids: continue
                    if any(x['id'] == sub_href for x in new_items): continue

                    new_items.append({
                        "source": f"MSR - {config['name']}",
                        "id": sub_href,
                        "title": full_title,
                        "date": date_display,
                        "link": sub_href
                    })
                    count_in_page += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[MSR] Lỗi tại {config['name']}: {e}")
            continue

    return new_items