import sys
import os
import json
import zipfile
import tempfile
import random
import threading
import shutil
from datetime import datetime
import requests
import time

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QLineEdit, QPushButton,
    QFileDialog, QHBoxLayout, QVBoxLayout, QMessageBox, QProgressBar, QDialog,
    QFormLayout
)
from PySide6.QtCore import Qt, Signal, Slot

import pandas as pd
from openpyxl import Workbook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re  
from order import process_order_zip

import google.auth
import google.auth.transport.requests
import google.oauth2.service_account
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# build command: pyinstaller --noconsole --onefile --icon=images/cashbot.ico main.py

CONFIG_FILE = "config.json"
LOCAL_VERSION = "1.0.1"  # 현재 프로그램 버전
VERSION_URL = "https://seunghoon4176.github.io/balzubot/version.json"

def get_drive_service():
    cred_path = os.path.join(os.path.dirname(__file__), "google_credentials.json")
    with open(cred_path, "r", encoding="utf-8") as f:
        credentials_dict = json.load(f)
    credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(
        credentials_dict,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("drive", "v3", credentials=creds)
    return service


def upload_folder_to_drive(folder_path, drive_folder_id="0AIUiN0FF2S3SUk9PVA"):
    service = get_drive_service()

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if not os.path.isfile(file_path):
            continue

        file_metadata = {
            "name": filename,
            "parents": [drive_folder_id],
        }

        media = MediaFileUpload(file_path, resumable=True)
        uploaded = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True  # 공유 드라이브 대응
        ).execute()
        print(f"✔ 업로드 완료: {filename} → File ID: {uploaded['id']}")

def check_version_or_exit():
    try:
        response = requests.get(VERSION_URL, timeout=5)
        if response.status_code == 200:
            data = response.json()
            remote_version = data.get("version", "")
            if remote_version != LOCAL_VERSION:
                QMessageBox.critical(
                    None,
                    "버전 오류",
                    f"현재 버전({LOCAL_VERSION})은 더 이상 사용할 수 없습니다。\n"
                    f"최신 버전({remote_version})으로 업데이트해주세요。"
                )
                sys.exit(1)
        else:
            QMessageBox.critical(None, "버전 확인 실패", "버전 정보를 불러오지 못했습니다。")
            sys.exit(1)
    except Exception as e:
        QMessageBox.critical(None, "버전 확인 오류", f"버전 확인 중 오류 발생：\n{str(e)}")
        sys.exit(1)


class SettingsDialog(QDialog):
    """
    쿠팡 로그인용 아이디/비밀번호와 브랜드명을 입력하고 저장하는 다이얼로그
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("로그인 설정")
        self.setFixedSize(300, 200)

        layout = QFormLayout(self)

        # 아이디 입력
        self.le_id = QLineEdit()
        layout.addRow("쿠팡 아이디:", self.le_id)

        # 비밀번호 입력 (숨김)
        self.le_pw = QLineEdit()
        self.le_pw.setEchoMode(QLineEdit.Password)
        layout.addRow("쿠팡 비밀번호:", self.le_pw)

        # 브랜드명 입력
        self.le_brand = QLineEdit()
        layout.addRow("브랜드명:", self.le_brand)

        # 저장 버튼
        btn_save = QPushButton("저장")
        btn_save.clicked.connect(self.save_credentials)
        layout.addWidget(btn_save)

        self.load_credentials()

    def load_credentials(self):
        """
        config.json 파일이 있으면 불러와서 입력란에 채워준다.
        """
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.le_id.setText(data.get("coupang_id", ""))
                self.le_pw.setText(data.get("coupang_pw", ""))
                self.le_brand.setText(data.get("brand_name", ""))
            except:
                pass

    def save_credentials(self):
        """
        현재 입력된 값을 config.json에 저장
        """
        coupang_id = self.le_id.text().strip()
        coupang_pw = self.le_pw.text().strip()
        brand_name = self.le_brand.text().strip()

        if not coupang_id or not coupang_pw:
            QMessageBox.warning(self, "경고", "아이디와 비밀번호를 모두 입력해주세요。")
            return

        data = {
            "coupang_id": coupang_id,
            "coupang_pw": coupang_pw,
            "brand_name": brand_name
        }
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            QMessageBox.information(self, "저장 완료", "설정이 저장되었습니다。")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "오류", f"저장 중 오류가 발생했습니다：\n{str(e)}")


class OrderApp(QMainWindow):
    # 크롤링 완료/에러 시그널
    crawlFinished = Signal(str)
    crawlError = Signal(str)
    progressUpdated = Signal(int)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("수강생 발주 프로그램")
        self.setFixedSize(650, 300)

        # 1) 발주 리스트 ZIP 경로
        self.order_zip_path = None
        # 2) 재고 리스트 Excel 경로
        self.inventory_xlsx_path = None
        # 3) 브랜드명
        self.brand_name = None

        # 쿠팡 로그인용 아이디/비밀번호 (config.json에서 불러오기)
        self.coupang_id = ""
        self.coupang_pw = ""

        # 발주데이터 저장 구조
        # { PO번호: {
        #      "barcode": …,
        #      "product_code": …,
        #      "product_name": …,
        #      "center": …,
        #      "eta": …,
        #      "shipment": …,
        #      "invoice": … (랜덤 10자리 숫자)
        #   }
        # }
        self.orders_data = {}
        self.cached_shipment = {}  # { "센터|입고일": shipment_no }

        # Selenium WebDriver 객체
        self.driver = None

        self.init_ui()
        self.load_config()

        # 시그널 연결
        self.progressUpdated.connect(self.on_progress_updated)
        self.crawlFinished.connect(self.on_crawl_finished)
        self.crawlError.connect(self.on_crawl_error)

    def init_ui(self):
        central = QWidget()
        layout = QVBoxLayout()

        # ─── 1) 발주 리스트 ZIP 선택 ───────────────────────────────────────
        h1 = QHBoxLayout()
        lbl1 = QLabel("1) 발주 리스트 ZIP:")
        self.le_zip = QLineEdit()
        self.le_zip.setReadOnly(True)
        btn_zip = QPushButton("파일 선택")
        btn_zip.clicked.connect(self.select_order_zip)
        h1.addWidget(lbl1)
        h1.addWidget(self.le_zip)
        h1.addWidget(btn_zip)

        # ─── 2) 재고 리스트(Excel) 선택 ────────────────────────────────────
        h2 = QHBoxLayout()
        lbl2 = QLabel("2) 재고 리스트(Excel):")
        self.le_inventory = QLineEdit()
        self.le_inventory.setReadOnly(True)
        btn_inventory = QPushButton("파일 선택")
        btn_inventory.clicked.connect(self.select_inventory_xlsx)
        h2.addWidget(lbl2)
        h2.addWidget(self.le_inventory)
        h2.addWidget(btn_inventory)

        # ─── 3) 브랜드명 입력 ─────────────────────────────────────────────
        h3 = QHBoxLayout()
        lbl3 = QLabel("3) 브랜드명:")
        self.le_brand = QLineEdit()
        h3.addWidget(lbl3)
        h3.addWidget(self.le_brand)

        # ─── 4) 로그인 설정 버튼 ───────────────────────────────────────────
        h4 = QHBoxLayout()
        lbl4 = QLabel("")
        btn_settings = QPushButton("설정")
        btn_settings.clicked.connect(self.open_settings_dialog)
        h4.addWidget(lbl4)
        h4.addWidget(btn_settings)
        h4.addStretch()

        # ─── 5) 일괄 처리 버튼 ─────────────────────────────────────────────
        h5 = QHBoxLayout()
        self.btn_batch = QPushButton("일괄 처리")
        self.btn_batch.clicked.connect(self.run_batch_pipeline)
        self.btn_batch.setEnabled(False)
        h5.addWidget(self.btn_batch)

        # ─── 진행 상태바 ────────────────────────────────────────────────
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setVisible(False)

        layout.addLayout(h1)
        layout.addLayout(h2)
        layout.addLayout(h3)
        layout.addLayout(h4)
        layout.addLayout(h5)
        layout.addWidget(self.progress)

        central.setLayout(layout)
        self.setCentralWidget(central)

        # 입력값 변경 시 “일괄 처리” 버튼 활성화 여부 갱신
        self.le_zip.textChanged.connect(self.toggle_batch_button)
        self.le_inventory.textChanged.connect(self.toggle_batch_button)
        self.le_brand.textChanged.connect(self.toggle_batch_button)

    @Slot(int)
    def on_progress_updated(self, value):
        """시그널로 받은 진행률을 QProgressBar에 적용"""
        self.progress.setValue(value)

    @Slot(str)
    def on_crawl_finished(self, message):
        """크롤링 완료 시 메인 스레드에서 처리 → 자동으로 주문서 생성까지 이어감"""
        self.progress.setVisible(False)
        QMessageBox.information(self, "크롤링 완료", message)

        # 주문서 생성 자동 호출
        try:
            self.generate_orders()
        except Exception as e:
            QMessageBox.critical(self, "주문서 생성 오류", f"주문서 생성 중 오류가 발생했습니다：\n{str(e)}")

        # 버튼 원복
        self.btn_batch.setText("일괄 처리")
        self.btn_batch.clicked.disconnect()
        self.btn_batch.clicked.connect(self.run_batch_pipeline)
        self.btn_batch.setEnabled(False)

    @Slot(str)
    def on_crawl_error(self, errmsg):
        """크롤링 중 에러 발생 시 메인 스레드에서 처리"""
        self.progress.setVisible(False)
        QMessageBox.critical(self, "크롤 오류", errmsg)
        if self.driver:
            self.driver.quit()
            self.driver = None

        # 버튼 원복
        self.btn_batch.setText("일괄 처리")
        self.btn_batch.clicked.disconnect()
        self.btn_batch.clicked.connect(self.run_batch_pipeline)
        self.btn_batch.setEnabled(True)

    def run_batch_pipeline(self):
        """일괄 처리 버튼 눌렀을 때 → Zero Phase → First Phase"""
        success = self.zero_phase()
        if success:
            self.first_phase()

    def load_config(self):
        """
        config.json에서 coupang_id, coupang_pw, brand_name 값을 읽어온다.
        """
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.coupang_id = data.get("coupang_id", "")
                self.coupang_pw = data.get("coupang_pw", "")
                self.brand_name = data.get("brand_name", "")
                self.le_brand.setText(self.brand_name)
            except:
                self.coupang_id = ""
                self.coupang_pw = ""
                self.brand_name = ""
        else:
            self.coupang_id = ""
            self.coupang_pw = ""
            self.brand_name = ""

    def open_settings_dialog(self):
        """
        쿠팡 로그인 설정 다이얼로그를 열어서 ID/PW를 입력받고 저장
        """
        dlg = SettingsDialog(self)
        if dlg.exec() == QDialog.Accepted:
            self.load_config()

    def toggle_batch_button(self):
        """발주 ZIP, 재고 Excel, 브랜드명 모두 입력되면 활성화"""
        if self.le_zip.text() and self.le_inventory.text() and self.le_brand.text():
            self.btn_batch.setEnabled(True)
        else:
            self.btn_batch.setEnabled(False)

    def select_order_zip(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "발주 리스트 ZIP 선택",
            "",
            "ZIP Files (*.zip)"
        )
        if path:
            self.order_zip_path = path
            self.le_zip.setText(path)

    def select_inventory_xlsx(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "재고 리스트(Excel) 선택",
            "",
            "Excel Files (*.xlsx *.xls)"
        )
        if path:
            self.inventory_xlsx_path = path
            self.le_inventory.setText(path)

    def zero_phase(self) -> bool:
        """
        process_order_zip()을 호출하여 ZIP 내부의 엑셀 파일을
        미리 처리한다. (order_processor 모듈 활용)
        """
        try:
            result = process_order_zip(self.order_zip_path)
            if result["failures"]:
                QMessageBox.warning(
                    self, "주의", "일부 파일 처리 실패：\n" + "\n".join(result["failures"])
                )
            else:
                QMessageBox.information(self, "Zero Phase 완료", "ZIP 파일 처리 및 엑셀 생성 완료")
            return True
        except Exception as e:
            print("Zero Phase 에러：", str(e))
            return False

    def first_phase(self):
        """
        ZIP 해제 및 발주 데이터 파싱 → Selenium 로그인 준비
        """
        try:
            print("=== First Phase 시작 ===")
            tmpdir = tempfile.mkdtemp(prefix="order_zip_")
            with zipfile.ZipFile(self.order_zip_path, 'r') as zf:
                excel_files = []

                # ZIPInfo마다 파일명 CP437→CP949 디코딩
                for zi in zf.infolist():
                    raw_name_bytes = zi.filename.encode('cp437')
                    try:
                        real_name = raw_name_bytes.decode('cp949')
                    except:
                        real_name = zi.filename

                    if real_name.endswith("/"):
                        continue

                    target_path = os.path.join(tmpdir, real_name)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)

                    with zf.open(zi) as source, open(target_path, 'wb') as target:
                        target.write(source.read())

                    if real_name.lower().endswith((".xls", ".xlsx")):
                        excel_files.append(target_path)

            if not excel_files:
                raise Exception("ZIP 내부에 Excel 파일이 없습니다。")

            self.orders_data.clear()
            self.cached_shipment.clear()

            for idx, xlsx in enumerate(excel_files):
                # (가) 파일 전체를 헤더 없이 읽어 PO번호/ETA/센터명 추출
                df_raw = pd.read_excel(xlsx, header=None, dtype=str)

                # ① “발주번호”가 있는 행을 찾아 PO번호 추출
                po_row = df_raw[
                    df_raw.iloc[:, 0].astype(str).str.contains("발주번호", na=False)
                ].index
                if len(po_row) == 0:
                    raise Exception(f"발주번호 행을 찾을 수 없습니다：{os.path.basename(xlsx)}")
                po_row = po_row[0]
                raw_po = df_raw.iloc[po_row, 2]
                po_no = str(raw_po).strip() if pd.notna(raw_po) else ""
                if not po_no:
                    raise Exception(f"발주번호가 비어 있습니다：{os.path.basename(xlsx)}")

                # ② “입고예정일시”가 있는 행을 찾아, 다음 행에서 ETA·센터명 추출
                eta_label_row = df_raw[
                    df_raw.iloc[:, 0].astype(str).str.contains("입고예정일시", na=False)
                ].index
                if len(eta_label_row) == 0:
                    raise Exception(f"입고예정일시 행을 찾을 수 없습니다：{os.path.basename(xlsx)}")
                eta_label_row = eta_label_row[0]
                data_row = eta_label_row + 1

                raw_eta = df_raw.iloc[data_row, 5]
                try:
                    eta = pd.to_datetime(raw_eta).to_pydatetime() if pd.notna(raw_eta) else None
                except:
                    eta = None

                raw_center = df_raw.iloc[data_row, 2]
                center = str(raw_center).strip() if pd.notna(raw_center) else ""

                # (나) 아이템 테이블 읽기 (헤더 = 19행 기준)
                df_items = pd.read_excel(xlsx, header=19, dtype=str)
                df_items = df_items.loc[:, ~df_items.columns.str.startswith("Unnamed")]
                df_items.columns = df_items.columns.str.strip()

                print(f"파일：{os.path.basename(xlsx)} 아이템 헤더：{df_items.columns.tolist()}")

                # “상품코드” 칼럼, “상품명/옵션/BARCODE” 칼럼 찾아두기
                col_product = next((c for c in df_items.columns if "상품코드" in c or "품번" in c), None)
                col_barcode = next((c for c in df_items.columns if "BARCODE" in c.upper()), None)

                if not col_product or not col_barcode:
                    raise Exception(
                        f"아이템 테이블에 '상품코드' 또는 'BARCODE' 칼럼이 없습니다。\n"
                        f"현재 칼럼：{df_items.columns.tolist()}"
                    )

                # (다) 첫 번째 블록(인덱스1→상품코드+상품명, 인덱스2→바코드)만 읽고 break
                product_name = ""
                barcode = ""
                product_code = ""

                if len(df_items) > 1:
                    raw_pc = df_items.iloc[1].get(col_product, "")
                    product_code = str(raw_pc).strip() if pd.notna(raw_pc) else ""

                    raw_pn = df_items.iloc[1].get(col_barcode, "")
                    product_name = str(raw_pn).strip() if pd.notna(raw_pn) else ""

                    if len(df_items) > 2:
                        raw_bc2 = df_items.iloc[2].get(col_barcode, "")
                        barcode = str(raw_bc2).strip() if pd.notna(raw_bc2) else ""

                # 디버그 출력
                print(f"[디버그] PO {po_no} → product_name：'{product_name}', barcode：'{barcode}', product_code：'{product_code}'")

                # orders_data에 저장 (shipment은 아직 None, invoice는 랜덤 10자리)
                self.orders_data[po_no] = {
                    "barcode": barcode or "",
                    "product_code": product_code or "",
                    "product_name": product_name or "",
                    "center": center or "",
                    "eta": eta,
                    "shipment": None,
                    "invoice": str(random.randint(10**9, 10**10 - 1))
                }

                percent = int((idx + 1) / len(excel_files) * 30)
                self.progressUpdated.emit(percent)

            # 파싱 완료 요약 출력
            num_orders = len(self.orders_data)
            print(f"파싱 완료：총 {num_orders}건의 발주 데이터를 읽었습니다。")
            for po_no, info in self.orders_data.items():
                print(f"   -> {po_no} | 바코드：'{info['barcode']}' | 상품명：'{info['product_name']}' | 송장：{info['invoice']}")

            QMessageBox.information(
                self, "파싱 완료",
                f"총 {num_orders}건의 발주 데이터를 읽었습니다。\n(콘솔창을 확인하세요.)"
            )
            if num_orders == 0:
                print(">>> orders_data가 비어있어 종료합니다。")
                return

            # ─── 2) Selenium WebDriver 생성 ─────────────────────────────────
            print("WebDriver 생성 준비")
            self.progress.setVisible(True)
            self.progressUpdated.emit(30)

            options = ChromeOptions()
            options.add_argument("--start-maximized")

            try:
                print("ChromeDriver 생성 시도")
                self.driver = webdriver.Chrome(options=options)
                print("ChromeDriver 생성 성공")
            except Exception as e:
                print("!!! WebDriver 생성 오류：", e)
                QMessageBox.critical(
                    self, "WebDriver 생성 오류",
                    f"ChromeDriver를 실행하지 못했습니다：\n{str(e)}\n\n"
                    "• ChromeDriver의 경로/버전을 확인하세요。\n"
                    "• 실행 파일을 직접 지정하려면 executable_path 인자를 추가하세요。"
                )
                return

            self.driver.implicitly_wait(5)

            print("로그인 페이지 열기")
            oauth_url = (
                "https://xauth.coupang.com/auth/realms/seller/"
                "protocol/openid-connect/auth?response_type=code&client_id=supplier-hub"
                "&scope=openid&state=IHkYZBuTHklLrJPsLrU1aIWS8TphAG9DaI_BVfIoHF0%3D"
                "&redirect_uri=https://supplier.coupang.com/login/oauth2/code/keycloak"
                "&nonce=goBP3HcUNx-B4Hi3dmaAvZ9730RKEwwHooFMOvPAXVg"
            )
            self.driver.get(oauth_url)

            # ─── 3) 자동 로그인 시도 (실패해도 수동 단계로) ───────────────────────
            if self.coupang_id and self.coupang_pw:
                try:
                    input_id = WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username']"))
                    )
                    input_pw = self.driver.find_element(By.CSS_SELECTOR, "input[name='password']")
                    input_id.clear()
                    input_id.send_keys(self.coupang_id)
                    input_pw.clear()
                    input_pw.send_keys(self.coupang_pw)

                    btn_login = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                    btn_login.click()
                    print("자동 로그인 시도 완료")
                except Exception:
                    print("자동 로그인 실패，수동 단계로 넘어갑니다。")
                    QMessageBox.information(
                        self, "알림",
                        "자동 로그인 시도 중 오류가 발생했습니다。\n"
                        "수동으로 로그인 후 “로그인 완료” 버튼을 눌러주세요。"
                    )
            else:
                print("저장된 쿠팡 계정 정보가 없습니다。")
                QMessageBox.information(
                    self, "알림",
                    "쿠팡 아이디/비밀번호가 설정되어 있지 않습니다。\n"
                    "브라우저에서 직접 로그인 완료 후 “로그인 완료” 버튼을 눌러주세요。"
                )

            # ─── 4) “로그인 완료” 버튼 준비 ─────────────────────────────────
            self.btn_batch.setText("로그인 완료")
            self.btn_batch.clicked.disconnect()
            self.btn_batch.clicked.connect(self.second_phase)
            self.btn_batch.setEnabled(True)

        except Exception as e:
            print("!!! first_phase 예외 발생：", e)
            self.crawlError.emit(f"초기 처리 중 오류가 발생했습니다：\n{str(e)}")

    def second_phase(self):
        """
        “로그인 완료” 클릭 시：
        1) 메뉴 클릭(“Logistics”→“Shipments”) 후 발주번호 입력창 대기 → 크롤링
        2) 발주확정 엑셀 생성(자동) 및 다운로드 파일 정리
        """
        self.btn_batch.setEnabled(False)
        self.progress.setVisible(True)

        threading.Thread(target=self.crawl_and_generate).start()

    def crawl_and_generate(self):
        try:
            driver = self.driver
            self.progressUpdated.emit(30)

            driver.get("https://supplier.coupang.com/dashboard/KR")

            # ── 1) 메뉴 진입
            try:
                btn_logistics = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/logistics']"))
                ); btn_logistics.click()

                btn_shipments = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/ibs/asn/active']"))
                ); btn_shipments.click()
            except:
                raise Exception("메뉴 클릭 실패 (Logistics → Shipments)")

            # ── 2) 발주번호 입력창
            try:
                search_input = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#purchaseOrderSeq"))
                )
            except:
                raise Exception("발주번호 입력창을 찾지 못했습니다.")

            download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            target_dir = os.path.join(os.getcwd(), "shipment")
            os.makedirs(target_dir, exist_ok=True)

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

            # ── 3) 다운로드 파일 이동
            time.sleep(5)
            dup_pat = re.compile(r"\s\(\d+\)(\.[^.]+)$")

            for fname in os.listdir(download_dir):
                low = fname.lower()
                if not (low.startswith("shipment_label_document") or
                        low.startswith("shipment_manifest_document")):
                    continue
                src = os.path.join(download_dir, fname)

                if dup_pat.search(fname):
                    try: os.remove(src)
                    except FileNotFoundError: pass
                    continue

                shutil.move(src, os.path.join(target_dir, fname))

            # ── 4) Google Drive 업로드
            try:
                upload_folder_to_drive(target_dir)
                print("📁 Google Drive 업로드 완료")
            except Exception as e:
                print(f"[경고] Google Drive 업로드 실패: {e}")

            driver.quit(); self.driver = None
            self.progressUpdated.emit(100)
            self.crawlFinished.emit("발주확정 라벨/매니페스트 다운로드 및 업로드 완료!")

        except Exception as e:
            print("crawl_and_generate 예외 발생:", e)
            self.crawlError.emit(str(e))

    def generate_orders(self):
        """
        재고 엑셀 → 3PL 신청서 / 주문서(부족분) 생성
        - 같은 바코드가 여러 Shipment에 걸쳐 등장해도
        ‘사용된 재고’를 누적 관리해 정확히 차감합니다.
        """
        try:
            # ── (1) 재고 파일 읽기 ───────────────────────────────────
            inv_df = (
                pd.read_excel(self.inventory_xlsx_path, dtype=str)
                .fillna("")
            )
            inventory_dict = {
                str(r["바코드"]).strip(): int(float(r.get("수량", 0) or 0))
                for _, r in inv_df.iterrows()
                if str(r.get("바코드", "")).strip()
            }

            # ── (2) 발주확정 엑셀 읽기 ────────────────────────────────
            confirm_path = os.path.join(os.getcwd(), "발주 확정 양식.xlsx")
            df_confirm = (
                pd.read_excel(confirm_path, dtype=str)
                .fillna("")
            )
            df_confirm["확정수량"] = (
                pd.to_numeric(df_confirm["확정수량"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

            # ① Shipment 매핑
            df_confirm["Shipment"] = df_confirm["발주번호"].map(
                lambda x: self.orders_data.get(str(x).strip(), {}).get("shipment", "")
            )

            # ② 더미 행 제거(확정수량 0)
            df_confirm = df_confirm[df_confirm["확정수량"] > 0]

            # ③ 그룹화
            group_cols = ["Shipment", "상품바코드", "상품이름", "물류센터", "입고예정일"]
            df_group = (
                df_confirm[group_cols + ["확정수량"]]
                .groupby(group_cols, as_index=False)["확정수량"]
                .sum()
            )

            # ── (3) 결과 워크북 준비 ──────────────────────────────────
            wb_3pl, wb_order = Workbook(), Workbook()
            ws_3pl, ws_order = wb_3pl.active, wb_order.active
            ws_3pl.title, ws_order.title = "3PL신청서", "주문서"

            # (a) 3PL 신청서 헤더 – 공란 열 3개
            header_3pl = [
                "브랜드명", "쉽먼트번호", "공란", "공란",
                "SKU(제품명)", "바코드", "수량", "공란",
                "입고예정일", "센터명"
            ]
            ws_3pl.append(header_3pl)

            # (b) 주문서 헤더
            ws_order.append([
                "바코드명", "바코드", "상품코드", "센터명",
                "쉽먼트번호", "발주번호", "입고예정일", "수량", "브랜드명"
            ])

            # ── (4) 행 쓰기 ─────────────────────────────────────────
            used_stock, brand = {}, self.le_brand.text().strip()

            for _, r in df_group.iterrows():
                bc       = str(r["상품바코드"]).strip()
                pname    = str(r["상품이름"]).strip()
                center   = str(r["물류센터"]).strip()
                ship_no  = r["Shipment"]
                eta_raw  = r["입고예정일"]

                try:
                    eta_str = pd.to_datetime(eta_raw).strftime("%Y-%m-%d")
                except Exception:
                    eta_str = ""

                qty = int(r["확정수량"])

                # ── 3PL 신청서 (템플릿 순서 그대로) ──
                ws_3pl.append([
                    brand,          # 0
                    ship_no,        # 1
                    "", "",         # 2,3  공란
                    pname,          # 4
                    bc,             # 5
                    qty,            # 6
                    "",             # 7  공란
                    eta_str,        # 8
                    center          # 9
                ])

                # ── 주문서(부족분만) ──
                already_used  = used_stock.get(bc, 0)
                avail_now     = inventory_dict.get(bc, 0) - already_used
                need_qty      = max(qty - max(avail_now, 0), 0)

                if need_qty > 0:
                    mask = (
                        (df_confirm["Shipment"] == ship_no) &
                        (df_confirm["상품바코드"] == bc)
                    )
                    product_code = po_no = ""
                    if mask.any():
                        product_code = str(df_confirm.loc[mask, "상품번호"].iloc[0]).strip()
                        po_no        = str(df_confirm.loc[mask, "발주번호"].iloc[0]).strip()

                    ws_order.append([
                        pname, bc, product_code, center,
                        ship_no, po_no, eta_str, need_qty, brand
                    ])

                used_stock[bc] = already_used + min(qty, max(avail_now, 0))

            # ── (5) 저장 ────────────────────────────────────────────
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            wb_3pl.save(f"3PL신청서_{ts}.xlsx")
            wb_order.save(f"주문서_{ts}.xlsx")

            QMessageBox.information(
                self, "완료",
                f"3PL 신청서와 주문서를 생성했습니다:\n- 3PL신청서_{ts}.xlsx\n- 주문서_{ts}.xlsx"
            )

        except Exception as e:
            QMessageBox.critical(self, "오류", f"주문서 생성 중 오류:\n{e}")



if __name__ == "__main__":
    app = QApplication(sys.argv)
    check_version_or_exit()
    window = OrderApp()
    window.show()
    sys.exit(app.exec())
