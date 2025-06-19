from PySide6.QtGui import QCloseEvent
import sys, os, json, zipfile, tempfile, random, threading, shutil, time, re
from datetime import datetime
import openpyxl
import requests
import pandas as pd
from openpyxl import Workbook
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QLineEdit, QPushButton,
    QFileDialog, QHBoxLayout, QVBoxLayout, QMessageBox, QProgressBar, QDialog,
    QFormLayout
)
from PySide6.QtCore import Signal
from PySide6.QtGui import QIcon

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ZIP 전처리 모듈
from order_processor import process_order_zip, is_confirmed_excel
import subprocess

import gspread
import google.auth
import google.auth.transport.requests
import google.oauth2.service_account
from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --------------------------------------------------------------
def load_credentials():
    if getattr(sys, 'frozen', False):
        # PyInstaller 실행 중
        base_dir = sys._MEIPASS
    else:
        base_dir = os.path.dirname(__file__)
    cred_path = os.path.join(base_dir, "google_credentials.json")
    with open(cred_path, "r", encoding="utf-8") as f:
        return json.load(f)

GOOGLE_CREDENTIALS_DICT = load_credentials()


# ─── 상수 ─────────────────────────────────────────────────────
CONFIG_FILE   = "config.json"

if getattr(sys, 'frozen', False):  # PyInstaller 실행 여부
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(__file__)

PRODUCT_XLSX = os.path.join(BASE_DIR, "상품정보.xlsx")

PRODUCT_HEADERS = [
    "상품바코드", "상품바코드명", "상품코드",
    "상품옵션1(중문)", "상품옵션2(중문)", "상품옵션3(중문)",
    "상품단가(위안)", "이미지URL", "상품URL",
    "통관품목명(영문)", "통관품목명(한글)",
    "소재(바코드표시)", "주의사항(바코드표시)",
    "포장1개당구매수량", "합포장여부", "메모"
]

STOCK_SHEET_CSV = (
    "https://docs.google.com/spreadsheets/d/"
    "1-HB7z7TmWoBhXPCXjp32biuYKB4ITxQfwdhQ_dO52l4/export"
    "?format=csv&gid=794212207"
)

ICON_PATH = os.path.join(os.path.dirname(__file__), "images", "cashbot.ico")

def find_column(df: pd.DataFrame, keywords: list) -> str | None:
    # 공백 제거 후 소문자 비교
    df_columns_cleaned = {col: col.strip().replace(" ", "").lower() for col in df.columns}
    for keyword in keywords:
        keyword_clean = keyword.strip().replace(" ", "").lower()
        for col, clean_col in df_columns_cleaned.items():
            if keyword_clean in clean_col:
                return col
    return None

def create_drive_folder(folder_name, parent_id=None):
    # 고정된 공유 폴더 ID 반환
    return "1onRPHGHDSAva4bQB6kjxQ5Z1v1SJ12xo"

def upload_folder_to_drive(folder_path, drive_folder_id):
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS_DICT, scopes=scopes)
    service = build("drive", "v3", credentials=creds)

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if not os.path.isfile(file_path):
            continue

        file_metadata = {
            "name": filename,
            "parents": [drive_folder_id],
        }

        media = MediaFileUpload(file_path, resumable=True)
        uploaded = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        print(f"✔ 업로드 완료: {filename} → Drive File ID: {uploaded['id']}")

def safe_strip(value):
    """None 또는 NaN을 안전하게 처리하여 문자열로 반환"""
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()

def load_stock_df(biz_num: str) -> pd.DataFrame:
    try:
        # 구글 인증 처리
        GOOGLE_CREDENTIALS_DICT["private_key"] = GOOGLE_CREDENTIALS_DICT["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS_DICT, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        client = gspread.authorize(creds)

        # 시트 접근
        sheet = client.open_by_key("1-HB7z7TmWoBhXPCXjp32biuYKB4ITxQfwdhQ_dO52l4")

        # ─────────────────────────────
        # ✅ 재고 리스트 처리
        ws_stock = sheet.worksheet("재고 리스트")
        data_stock = ws_stock.get_all_values()
        header = data_stock[0]
        records = data_stock[1:]

        print(f"[DEBUG] 열 개수: {len(header)}")

        df_stock = pd.DataFrame(records, columns=header).fillna("")

        # 열 이름 유연하게 찾기
        def find_column(possible_names: list[str]) -> str | None:
            for key in possible_names:
                for col in df_stock.columns:
                    if key.strip().lower() in col.strip().lower():
                        return col
            return None

        sku_col  = find_column(["SKU", "상품코드"])
        name_col = find_column(["제품명", "상품명"])
        bc_col   = find_column(["바코드", "barcode"])
        qty_col  = find_column(["수량", "재고", "재고수량"])
        biz_col  = find_column(["사업자 번호", "사업자", "사업자등록번호"])

        # 필수 열 확인
        if not all([sku_col, name_col, bc_col, qty_col, biz_col]):
            print("[재고 시트 오류] 필수 열 누락 - SKU, 제품명, 바코드, 수량, 사업자번호 중 하나가 없습니다.")
            return pd.DataFrame(columns=["SKU", "상품명", "바코드", "수량"])

        # 사업자 필터링
        df_filtered = df_stock[df_stock[biz_col].astype(str).str.strip() == biz_num]

        if df_filtered.empty:
            print(f"[INFO] 재고 시트에 해당 사업자번호 {biz_num} 에 대한 데이터 없음")
            return pd.DataFrame(columns=["SKU", "상품명", "바코드", "수량"])

        df_result = df_filtered[[sku_col, name_col, bc_col, qty_col]]
        df_result.columns = ["SKU", "상품명", "바코드", "수량"]

        # 저장
        ts = datetime.now().strftime("%Y%m%d")
        filename = f"재고_{biz_num}_{ts}.xlsx"
        df_result.to_excel(filename, index=False)
        print(f"[INFO] 재고 저장 완료: {filename}")

        # ─────────────────────────────
        # ✅ 입출고 리스트 처리
        try:
            ws_inout = sheet.worksheet("입출고 리스트")
            data_inout = ws_inout.get_all_values()
            df_inout = pd.DataFrame(data_inout[1:], columns=data_inout[0]).fillna("")

            biz_col_io = next((c for c in df_inout.columns if "사업자 번호" in c), None)
            if biz_col_io:
                df_filtered_io = df_inout[df_inout[biz_col_io].astype(str).str.strip() == biz_num]

                if not df_filtered_io.empty:
                    io_filename = f"입출고리스트_{biz_num}_{ts}.xlsx"
                    df_filtered_io.to_excel(io_filename, index=False)
                    print(f"[INFO] 입출고리스트 저장 완료: {io_filename}")
        except Exception as e_io:
            print(f"[WARN] 입출고리스트 시트 처리 중 오류: {e_io}")

        return df_result

    except Exception as e:
        print("[load_stock_df 예외 발생]", type(e), e)
        return pd.DataFrame(columns=["SKU", "상품명", "바코드", "수량"])


# ─── 설정 다이얼로그 ─────────────────────────────────────────
class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("쿠팡 ID/PW 설정")
        self.setFixedSize(320, 200)
        self.setWindowIcon(QIcon(ICON_PATH))
        lay = QFormLayout(self)
        
        self.le_biz = QLineEdit(); lay.addRow("사업자번호:", self.le_biz)
        self.le_id = QLineEdit();  lay.addRow("쿠팡 아이디:", self.le_id)
        self.le_pw = QLineEdit();  self.le_pw.setEchoMode(QLineEdit.Password)
        lay.addRow("쿠팡 비밀번호:", self.le_pw)
        self.le_brand = QLineEdit(); lay.addRow("브랜드명:", self.le_brand)

        btn = QPushButton("저장"); btn.clicked.connect(self._save); lay.addWidget(btn)
        self._load()

    def _load(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.le_biz.setText(d.get("business_number", "")) 
            self.le_id.setText(d.get("coupang_id", ""))
            self.le_pw.setText(d.get("coupang_pw", ""))
            self.le_brand.setText(d.get("brand_name", ""))

    def _save(self):
        if not self.le_id.text().strip() or not self.le_pw.text().strip():
            QMessageBox.warning(self, "경고", "쿠팡 ID/PW를 입력하세요."); return
        if not self.le_biz.text().strip():
            QMessageBox.warning(self, "경고", "사업자번호를 입력하세요."); return
        data = {
            "business_number": self.le_biz.text().strip(), 
            "coupang_id": self.le_id.text().strip(),
            "coupang_pw": self.le_pw.text().strip(),
            "brand_name": self.le_brand.text().strip(),
        }
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.accept()
        parent = self.parent()
        if parent and hasattr(parent, "_enable_run"):
            parent._load_config()  


class UpdateWindow(QWidget):
    progressChanged = Signal(int)  # ✅ 시그널 정의

    def __init__(self, update_url, parent=None):
        super().__init__(parent)
        self.setWindowTitle("업데이트 중...")
        self.setFixedSize(300, 100)
        self.progress = QProgressBar(self)
        self.progress.setRange(0, 100)
        
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("업데이트 중입니다. 잠시만 기다려주세요."))
        layout.addWidget(self.progress)

        self.progressChanged.connect(self.progress.setValue)  # ✅ 시그널 → UI 연결

        self.show()

        # 업데이트 쓰레드 실행
        threading.Thread(target=self.perform_update_auto, args=(update_url,), daemon=True).start()

    def perform_update_auto(self, update_url):
        try:
            # 현재 exe가 있는 디렉토리 기준으로 다운로드 및 압축 해제
            base_dir = os.path.dirname(sys.argv[0])
            zip_path = os.path.join(base_dir, "balzubot_update.zip")
            extract_dir = os.path.join(base_dir, "balzubot_new")

            with requests.get(update_url, stream=True) as r:
                r.raise_for_status()
                total = int(r.headers.get('content-length', 0))
                downloaded = 0
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            percent = int(downloaded / total * 100)
                            self.progressChanged.emit(min(percent, 99))

            # 압축 해제
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)
            shutil.unpack_archive(zip_path, extract_dir)

            # 실행 파일 찾기 및 실행
            exe_files = [f for f in os.listdir(extract_dir) if f.endswith(".exe")]
            if not exe_files:
                self._show_error("실행 파일을 찾을 수 없습니다.")
                return

            self.progressChanged.emit(100)
            time.sleep(0.5)

            exe_path = os.path.join(extract_dir, exe_files[0])
            subprocess.Popen([exe_path])
            time.sleep(1)
            os._exit(0)

        except Exception as e:
            self._show_error(f"업데이트 실패: {e}")
            os._exit(1)

    def _show_error(self, msg):
        # ✅ 메시지박스는 메인 스레드에서 실행되도록 시그널로 처리해도 좋지만, 여기선 최소화 위해 직접 사용
        QMessageBox.critical(self, "업데이트 오류", msg)

    def closeEvent(self, event):
        os._exit(1)


# ─── 메인 윈도우 ────────────────────────────────────────────
class OrderApp(QMainWindow):

    crawlFinished   = Signal(str)
    crawlError      = Signal(str)
    progressUpdated = Signal(int)

    def __init__(self):
        super().__init__()

        self.business_number = ""

        self.setWindowTitle("수강생 발주 프로그램")
        self.setFixedSize(680, 300)
        self.setWindowIcon(QIcon(ICON_PATH))

        # 설정값
        self.order_zip_path = None
        self.coupang_id = self.coupang_pw = ""
        self.brand_name = ""

        # 런타임
        self.orders_data = {}
        self.cached_shipment = {}
        self.driver = None

        self._build_ui(); self._load_config()
        self.progressUpdated.connect(lambda v: self.progress.setValue(v))
        self.crawlFinished.connect(self._crawl_ok)
        self.crawlError.connect(self._crawl_err)


    # UI ----------------------------------------------------------------
    def _build_ui(self):
        cen = QWidget(); self.setCentralWidget(cen)
        lay = QVBoxLayout(cen)

        # ZIP
        row_zip = QHBoxLayout(); row_zip.addWidget(QLabel("발주 ZIP:"))
        self.le_zip = QLineEdit(); self.le_zip.setReadOnly(True)
        btn_zip = QPushButton("파일 선택"); btn_zip.clicked.connect(self._pick_zip)
        row_zip.addWidget(self.le_zip); row_zip.addWidget(btn_zip)

        # 브랜드
        row_brand = QHBoxLayout(); row_brand.addWidget(QLabel("브랜드명:"))
        self.le_brand = QLineEdit(); row_brand.addWidget(self.le_brand)

        # 설정
        row_set = QHBoxLayout(); row_set.addStretch()
        btn_set = QPushButton("쿠팡 ID/PW 설정"); btn_set.clicked.connect(self._open_settings)
        row_set.addWidget(btn_set)

        # 실행
        row_run = QHBoxLayout()
        self.btn_run = QPushButton("일괄 처리"); self.btn_run.clicked.connect(self._run_pipeline)
        self.btn_run.setEnabled(False); row_run.addWidget(self.btn_run)
        self.btn_batch = self.btn_run

        row_download = QHBoxLayout()
        btn_download = QPushButton("재고/입출고 다운로드")
        btn_download.clicked.connect(self._download_stock_io)
        row_download.addWidget(btn_download)
        lay.addLayout(row_download)

        # progress
        self.progress = QProgressBar(); self.progress.setRange(0, 100); self.progress.setVisible(False)

        for r in (row_zip, row_brand, row_set, row_run): lay.addLayout(r)
        lay.addWidget(self.progress)

        for w in (self.le_zip, self.le_brand): w.textChanged.connect(self._enable_run)


    def _download_stock_io(self):
        if not self.business_number:
            QMessageBox.warning(self, "사업자번호 없음", "먼저 설정에서 사업자번호를 입력하세요.")
            return
        try:
            result_df = load_stock_df(self.business_number)
            if result_df.empty:
                QMessageBox.information(self, "완료", "해당 사업자의 재고 데이터가 없습니다.")
            else:
                QMessageBox.information(self, "완료", "재고 및 입출고 리스트 다운로드가 완료되었습니다.")
        except Exception as e:
            QMessageBox.critical(self, "오류", f"다운로드 중 오류 발생: {e}")
            
    def _enable_run(self):
        self.btn_run.setEnabled(bool(self.le_zip.text() and self.le_brand.text() and self.business_number))

    # 설정 로드 ----------------------------------------------------------
    def _load_config(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.coupang_id = d.get("coupang_id", "")
            self.coupang_pw = d.get("coupang_pw", "")
            self.brand_name = d.get("brand_name", "")
            self.business_number = d.get("business_number", "")
            self.le_brand.setText(self.brand_name)
        self._enable_run()

    # UI slots ----------------------------------------------------------
    def _pick_zip(self):
        p, _ = QFileDialog.getOpenFileName(self, "발주 ZIP 선택", "", "ZIP Files (*.zip)")
        if p:
            self.order_zip_path = p
            self.le_zip.setText(p)

    def _open_settings(self):
        if SettingsDialog(self).exec() == QDialog.Accepted:
            self._load_config()

    # ──────────────────────────────────────────────────────────
    # 파이프라인 시작
    # ──────────────────────────────────────────────────────────
    def _run_pipeline(self):
        # 0) 상품정보 엑셀 확인
        if not os.path.exists(PRODUCT_XLSX):
            wb = Workbook(); wb.active.title = "상품정보"; wb.active.append(PRODUCT_HEADERS)
            wb.save(PRODUCT_XLSX)
            QMessageBox.information(
                self, "상품정보 템플릿 생성",
                "상품정보.xlsx 파일이 없어 템플릿을 만들었습니다.\n"
                "상품 데이터를 입력한 뒤 다시 실행해 주세요."
            )
            return

        if self._zero_phase():       # ZIP 해제·발주서 추출
            self._first_phase()      # 바코드 검증 → Selenium 실행 준비

    # 0) ZIP 전처리 ------------------------------------------------------
    def _zero_phase(self):
        try:
            res = process_order_zip(self.order_zip_path)
            if res["failures"]:
                QMessageBox.warning(self, "주의", "일부 파일 처리 실패:\n" + "\n".join(res["failures"]))
            else:
                QMessageBox.information(self, "Zero Phase", "ZIP 파일 처리 완료.")
            return True
        except Exception as e:
            print("Zero Phase 오류:", e)
            return False

    # 1) 발주서 파싱 + 바코드 검증 + Selenium --------------------------------
    def _first_phase(self):
        try:
            print("first phase 시작")

            # 1-A. ZIP 해제 및 발주서 파싱
            tmpdir = tempfile.mkdtemp(prefix="order_zip_")
            excel_files = []
            confirmed_skipped = 0

            with zipfile.ZipFile(self.order_zip_path, 'r') as zf:
                for zi in zf.infolist():
                    raw = zi.filename.encode('cp437')
                    try:
                        real_name = raw.decode('cp949')
                    except UnicodeDecodeError:
                        real_name = zi.filename
                    if real_name.endswith("/"):
                        continue

                    target = os.path.join(tmpdir, real_name)
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                    with zf.open(zi) as src, open(target, 'wb') as dst:
                        dst.write(src.read())

                    if real_name.lower().endswith((".xls", ".xlsx")):
                        if is_confirmed_excel(target):
                            os.remove(target)
                            confirmed_skipped += 1
                            continue
                        excel_files.append(target)

            if not excel_files:
                msg = (
                    f"모든 엑셀 파일이 발주 확정본으로 제외되었습니다. ({confirmed_skipped}건)"
                    if confirmed_skipped > 0 else "미확정 발주서가 없습니다."
                )
                QMessageBox.information(self, "안내", msg)
                return

            self.orders_data.clear()

            for idx, xlsx in enumerate(excel_files):
                df_raw = pd.read_excel(xlsx, header=None, dtype=str)
                po_row = df_raw[df_raw.iloc[:, 0].astype(str).str.contains("발주번호", na=False)].index[0]
                po_no  = str(df_raw.iloc[po_row, 2]).strip()

                eta_row = df_raw[df_raw.iloc[:, 0].astype(str).str.contains("입고예정일시", na=False)].index[0] + 1
                eta_raw = df_raw.iloc[eta_row, 5]
                eta     = pd.to_datetime(eta_raw, errors="coerce")
                if pd.isna(eta):
                    raise ValueError(f"입고예정일시 변환 오류: {eta_raw}")
                eta = eta.to_pydatetime()

                center = str(df_raw.iloc[eta_row, 2]).strip()

                df_items = pd.read_excel(xlsx, header=19, dtype=str).fillna("")
                df_items = df_items.loc[:, ~df_items.columns.str.startswith("Unnamed")]
                df_items.columns = df_items.columns.str.strip()

                col_barcode = next((c for c in df_items.columns if "BARCODE" in c.upper() or "바코드" in c), None)
                if not col_barcode:
                    continue

                rows = df_items[col_barcode].tolist()
                valid_pairs = []
                i = 0
                while i < len(rows) - 1:
                    name = str(rows[i]).strip()
                    barcode = str(rows[i + 1]).strip()
                    if barcode.startswith("R"):
                        valid_pairs.append((name, barcode))
                        i += 2
                    else:
                        i += 1

                for product_name, barcode in valid_pairs:
                    if not barcode:
                        continue

                    if po_no not in self.orders_data:
                        self.orders_data[po_no] = {
                            "barcode":      barcode,
                            "product_code": "",
                            "product_name": product_name,
                            "center":       center,
                            "eta":          eta,
                            "shipment":     None,
                            "invoice":      str(random.randint(10**9, 10**10-1))
                        }

                pct = int((idx + 1) / len(excel_files) * 30)
                self.progressUpdated.emit(pct)

            print("first phase 중간체크3")

            # 🔍 상품정보 바코드 누락 자동 추가 (orders_data 안 씀)
            prod_df = pd.read_excel(PRODUCT_XLSX, dtype=str).fillna("")
            if "상품바코드" not in prod_df.columns:
                raise Exception("상품정보.xlsx에 '상품바코드' 열이 없습니다.")

            known_barcodes = set(prod_df["상품바코드"].astype(str).str.strip().str.lower())
            new_barcodes = []

            for xlsx in excel_files:
                df_items = pd.read_excel(xlsx, header=19, dtype=str).fillna("")
                df_items = df_items.loc[:, ~df_items.columns.str.startswith("Unnamed")]
                df_items.columns = df_items.columns.str.strip()

                col_barcode = next((c for c in df_items.columns if "BARCODE" in c.upper() or "바코드" in c), None)
                if not col_barcode:
                    continue

                rows = df_items[col_barcode].tolist()
                valid_pairs = []
                i = 0
                while i < len(rows) - 1:
                    name = str(rows[i]).strip()
                    barcode = str(rows[i + 1]).strip()
                    if barcode.startswith("R"):
                        valid_pairs.append((name, barcode))
                        i += 2
                    else:
                        i += 1

                for product_name, barcode in valid_pairs:
                    if not barcode:
                        continue
                    bc_lower = barcode.lower()
                    if bc_lower not in known_barcodes:
                        new_barcodes.append((barcode, product_name))

            # ✅ 중복 제거
            added = set()
            rows_to_append = []
            for barcode, name in new_barcodes:
                bc_lower = barcode.lower()
                if bc_lower not in added:
                    row = [barcode, name, ""] + [""] * (len(PRODUCT_HEADERS) - 3)
                    rows_to_append.append(row)
                    added.add(bc_lower)

            if rows_to_append:
                wb = openpyxl.load_workbook(PRODUCT_XLSX)
                ws = wb.active
                for row in rows_to_append:
                    ws.append(row)
                wb.save(PRODUCT_XLSX)

                QMessageBox.information(
                    self, "상품정보 자동 추가",
                    f"{len(rows_to_append)}개 바코드를 상품정보.xlsx에 자동으로 추가했습니다.\n내용 확인 후 다시 실행해주세요."
                )
                return

            print("first phase 중간체크4")

            # 1-C. 재고 확인
            try:
                inv_df = load_stock_df(self.business_number)
                if inv_df.empty:
                    QMessageBox.warning(self, "재고 시트 비어 있음", "현재 재고 시트에 데이터가 없습니다.\n계속 진행은 가능하지만 재고 확인은 생략됩니다.")
            except Exception as e:
                QMessageBox.warning(self, "재고 확인 경고", f"재고 정보를 불러오는 중 오류 발생: {e}\n재고 확인을 생략하고 계속 진행합니다.")

            # Selenium 로그인
            self.progress.setVisible(True)
            self.progressUpdated.emit(30)

            options = ChromeOptions()
            options.add_argument("--start-maximized")
            try:
                self.driver = webdriver.Chrome(options=options)
            except Exception as e:
                QMessageBox.critical(self, "WebDriver 오류", f"ChromeDriver 실행 실패:\n{e}")
                return

            self.driver.implicitly_wait(5)
            oauth_url = (
                "https://xauth.coupang.com/auth/realms/seller/"
                "protocol/openid-connect/auth?response_type=code&client_id=supplier-hub"
                "&scope=openid&state=abc&redirect_uri=https://supplier.coupang.com/login/oauth2/code/keycloak"
            )
            self.driver.get(oauth_url)

            if self.coupang_id and self.coupang_pw:
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username']"))
                    ).send_keys(self.coupang_id)
                    self.driver.find_element(By.CSS_SELECTOR, "input[name='password']").send_keys(self.coupang_pw)
                    self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
                except Exception:
                    pass

            self.btn_batch.setText("로그인 완료")
            self.btn_batch.clicked.disconnect()
            self.btn_batch.clicked.connect(self.second_phase)
            self.btn_batch.setEnabled(True)

        except Exception as e:
            print("[예외 - first_phase]", e)
            self.crawlError.emit(str(e))

    # ──────────────────────────────────────────────────────────
    # 2) Selenium 로그인 완료 후 크롤링
    # ──────────────────────────────────────────────────────────
    def second_phase(self):
        self.btn_batch.setEnabled(False)
        self.progress.setVisible(True)
        print("세컨드 페이즈 1")
        threading.Thread(target=self.crawl_and_generate).start()
        print("세컨드 페이즈 2")

    def crawl_and_generate(self):
        try:
            driver = self.driver
            self.progressUpdated.emit(30)

            driver.get("https://supplier.coupang.com/dashboard/KR")

            # 2-1) Logistics → Shipments 메뉴 진입
            try:
                btn_logistics = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/logistics']"))
                ); btn_logistics.click()

                btn_shipments = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/ibs/asn/active']"))
                ); btn_shipments.click()
            except Exception:
                raise Exception("메뉴 클릭 실패 (Logistics → Shipments)")

            # 2-2) 발주번호 입력창 확인
            try:
                search_input = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#purchaseOrderSeq"))
                )
            except:
                raise Exception("발주번호 입력창을 찾지 못했습니다.")

            # 2-3) 다운로드 폴더
            download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            target_dir   = os.path.join(os.getcwd(), "shipment"); os.makedirs(target_dir, exist_ok=True)

            # 2-4) 주문별 라벨/매니페스트 다운로드
            total = len(self.orders_data)
            for idx, (po_no, info) in enumerate(self.orders_data.items()):
                search_input.clear(); search_input.send_keys(po_no)
                driver.find_element(By.CSS_SELECTOR, "button#shipment-search-btn").click()

                try:
                    first_td = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR,
                            "table#parcel-tab tbody tr:first-child td:first-child"))
                    ); shipment_no = first_td.text.strip()
                except:
                    shipment_no = ""

                center, eta = info["center"], info["eta"]
                key = f"{center}|{eta.strftime('%Y-%m-%d') if eta else ''}"
                self.cached_shipment[key] = shipment_no
                self.orders_data[po_no]["shipment"] = shipment_no

                if shipment_no:
                    try:
                        driver.execute_script(
                            f"window.open('https://supplier.coupang.com/ibs/shipment/parcel/"
                            f"pdf-label/generate?parcelShipmentSeq={shipment_no}', '_blank');"
                        ); time.sleep(1.5)
                        driver.execute_script(
                            f"window.open('https://supplier.coupang.com/ibs/shipment/parcel/"
                            f"pdf-manifest/generate?parcelShipmentSeq={shipment_no}', '_blank');"
                        ); time.sleep(1.5)
                    except Exception as e:
                        print(f"[경고] {shipment_no} 다운로드 중 오류: {e}")

                percent = 30 + int((idx + 1) / total * 40)
                self.progressUpdated.emit(percent)

            # 2-5) 다운로드 완료 대기 후 정리
            time.sleep(5)
            dup_pat = re.compile(r"\s\(\d+\)(\.[^.]+)$")

            for fname in os.listdir(download_dir):
                low = fname.lower()
                if not (low.startswith("shipment_label_document") or
                        low.startswith("shipment_manifest_document")):
                    continue
                src = os.path.join(download_dir, fname)

                if dup_pat.search(fname):   # 중복본 삭제
                    try: os.remove(src)
                    except FileNotFoundError: pass
                    continue

                shutil.move(src, os.path.join(target_dir, fname))

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            drive_folder_name = f"shipment_{ts}"

            try:
                drive_folder_id = create_drive_folder("shipment")  # 아무 이름 넣어도 됨
                upload_folder_to_drive(target_dir, drive_folder_id)
                print(f"📁 Google Drive 업로드 완료: 공유폴더")
            except Exception as e:
                print(f"[Google Drive 업로드 실패] {e}")

            driver.quit(); self.driver = None
            self.progressUpdated.emit(100)
            self.crawlFinished.emit("발주확정 파일(라벨·매니페스트)이 모두 생성되었습니다.")

        except Exception as e:
            print("crawl_and_generate 예외 발생:", e)
            self.crawlError.emit(str(e))

    # ──────────────────────────────────────────────────────────
    # 3) 3PL 신청서 & 주문서 생성
    # ──────────────────────────────────────────────────────────
    def generate_orders(self):

        def append_to_google_sheet(sheet_id: str, sheet_name: str, brand: str, rows: list[list[str]]):
            scopes = ["https://www.googleapis.com/auth/spreadsheets"]
            creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS_DICT, scopes=scopes)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(sheet_id)
            worksheet = sheet.worksheet(sheet_name)

            content_rows = rows[1:]  # 헤더 제외
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for i, row in enumerate(content_rows):
                row.append(now_str if i == 0 else "")  # 첫 줄에만 시간

            worksheet.append_rows(content_rows, value_input_option="USER_ENTERED")

        try:
            inv_df = load_stock_df(self.business_number)
            inventory_dict = {
                str(r["바코드"]).strip(): int(float(r["수량"] or 0))
                for _, r in inv_df.iterrows()
            }
            used_stock = {}

            prod_df = pd.read_excel("상품정보.xlsx", dtype=str).fillna("")

            confirm_path = "발주 확정 양식.xlsx"
            df_confirm = pd.read_excel(confirm_path, dtype=str).fillna("")
            df_confirm["확정수량"] = pd.to_numeric(df_confirm["확정수량"], errors="coerce").fillna(0).astype(int)
            df_confirm["Shipment"] = df_confirm["발주번호"].map(
                lambda x: self.orders_data.get(str(x).strip(), {}).get("shipment", "")
            )
            df_confirm = df_confirm[df_confirm["확정수량"] > 0]

            group_cols = ["Shipment", "상품바코드", "상품이름", "물류센터", "입고예정일"]
            df_group = df_confirm[group_cols + ["확정수량"]].groupby(group_cols, as_index=False)["확정수량"].sum()

            brand = self.le_brand.text().strip()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")

            headers_3pl = ["브랜드명", "쉽먼트번호", "발주번호", "SKU번호",
                        "SKU(제품명)", "바코드", "수량", "입고예정일", "센터명"]
            rows_3pl = [headers_3pl]

            headers_order = ["바코드명", "바코드", "상품코드", "쿠팡납품센터명",
                            "쿠팡쉽먼트번호", "쿠팡입고예정일자", "입고마감준수여부", "발주수량", "중국재고사용여부"]
            rows_order = [headers_order]

            wb_3pl = Workbook()
            ws_3pl = wb_3pl.active
            ws_3pl.title = "3PL신청서"
            ws_3pl.append(headers_3pl)

            wb_order = Workbook()
            ws_order = wb_order.active
            ws_order.title = "주문서"
            ws_order.append(headers_order)

            for _, r in df_group.iterrows():
                bc = safe_strip(r.get("상품바코드"))
                pname = r["상품이름"]
                center = r["물류센터"]
                ship_no = r["Shipment"]
                eta_raw = r["입고예정일"]
                qty = int(r["확정수량"])
                eta_str = pd.to_datetime(eta_raw, errors="coerce").strftime("%Y-%m-%d") if eta_raw else ""

                mask = (df_confirm["Shipment"] == ship_no) & (df_confirm["상품바코드"] == bc)
                po_no = product_code = ""
                if mask.any():
                    po_no = str(df_confirm.loc[mask, "발주번호"].iloc[0]).strip()
                    product_code = str(df_confirm.loc[mask, "상품번호"].iloc[0]).strip()

                row_3pl = [brand, ship_no, po_no, product_code, pname, bc, qty, eta_str, center]
                
                rows_3pl.append(row_3pl)
                ws_3pl.append(row_3pl)

                already = used_stock.get(bc, 0)
                avail = inventory_dict.get(bc, 0) - already
                need = max(qty - max(avail, 0), 0)

                if need > 0:
                    row_order = [pname, bc, product_code, center, ship_no, eta_str, "Y", need, "N"]
                    rows_order.append(row_order)
                    ws_order.append(row_order)

                used_stock[bc] = already + min(qty, max(avail, 0))

            # ✅ 스프레드시트 전송
            append_to_google_sheet(
                sheet_id="1-HB7z7TmWoBhXPCXjp32biuYKB4ITxQfwdhQ_dO52l4",
                sheet_name="CALL 요청서",
                brand=brand,
                rows=rows_3pl
            )

            if len(rows_order) == 1:
                # ✅ 주문할 항목 없음
                ws_order.cell(row=2, column=1).value = "재고가 충분하여 주문할 항목이 없습니다."
                append_to_google_sheet(
                    sheet_id="1-HB7z7TmWoBhXPCXjp32biuYKB4ITxQfwdhQ_dO52l4",
                    sheet_name="CALL 주문서",
                    brand=brand,
                    rows=[["재고가 충분하여 주문할 항목이 없습니다."]]
                )
            else:
                append_to_google_sheet(
                    sheet_id="1-HB7z7TmWoBhXPCXjp32biuYKB4ITxQfwdhQ_dO52l4",
                    sheet_name="CALL 주문서",
                    brand=brand,
                    rows=rows_order
                )

            # ✅ 파일 저장
            wb_3pl.save(f"3PL신청내역_{ts}.xlsx")
            wb_order.save(f"주문서_{ts}.xlsx")

            QMessageBox.information(
                self, "완료",
                f"스프레드시트 전송 완료!\n"
                f"파일도 저장했습니다:\n"
                f"- 3PL신청내역_{ts}.xlsx\n"
                f"- 주문서_{ts}.xlsx"
            )

        except Exception as e:
            QMessageBox.critical(self, "오류", f"주문서 생성 중 오류:\n{e}")

    # ──────────────────────────────────────────────────────────
    # 크롤 완료/오류 콜백 및 버튼 리셋
    # ──────────────────────────────────────────────────────────
    def _crawl_ok(self, msg: str):
        self.progress.setVisible(False); QMessageBox.information(self, "크롤 완료", msg)
        try: self.generate_orders()
        except Exception as e: QMessageBox.critical(self, "주문서 오류", str(e))
        self._reset_btn()

    def _crawl_err(self, msg: str):
        self.progress.setVisible(False); QMessageBox.critical(self, "크롤 오류", msg)
        if self.driver: self.driver.quit(); self.driver = None
        self._reset_btn()

    def _reset_btn(self):
        self.btn_run.setText("일괄 처리")
        self.btn_run.clicked.disconnect(); self.btn_run.clicked.connect(self._run_pipeline)
        self.btn_run.setEnabled(True)


# ─── main ───────────────────────────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)

    try:
        VERSION_URL = "http://114.207.245.49/version"
        LOCAL_VERSION = "1.0.4"
        r = requests.get(VERSION_URL, timeout=5)
        if r.status_code == 200:
            data = r.json()
            latest_ver = data.get("balzubotversion")
            update_url = data.get("balzubot_update_url")
            if latest_ver and update_url and latest_ver != LOCAL_VERSION:
                update_win = UpdateWindow(update_url)
                sys.exit(app.exec())

    except Exception as e:
        QMessageBox.critical(None, "업데이트 오류", str(e))
        sys.exit(1)

    # 최신이면 본 프로그램 실행
    win = OrderApp()
    win.show()
    sys.exit(app.exec())