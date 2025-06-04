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

from order_processor import process_order_zip

#build command: pyinstaller --noconsole --onefile --icon=images/cashbot.ico main.py

CONFIG_FILE = "config.json"
LOCAL_VERSION = "1.0.0"  # 현재 프로그램 버전
VERSION_URL = "https://seunghoon4176.github.io/balzubot/version.json"

def check_version_or_exit():
    try:
        response = requests.get(VERSION_URL, timeout=5)
        if response.status_code == 200:
            data = response.json()
            remote_version = data.get("version", "")
            if remote_version != LOCAL_VERSION:
                QMessageBox.critical(None, "버전 오류", f"현재 버전({LOCAL_VERSION})은 사용할 수 없습니다.\n최신 버전({remote_version})으로 업데이트해주세요.")
                sys.exit(1)
        else:
            QMessageBox.critical(None, "버전 확인 실패", "버전 정보를 불러오지 못했습니다.")
            sys.exit(1)
    except Exception as e:
        QMessageBox.critical(None, "버전 확인 오류", f"버전 확인 중 오류 발생:\n{str(e)}")
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
            QMessageBox.warning(self, "경고", "아이디와 비밀번호를 모두 입력해주세요.")
            return

        data = {
            "coupang_id": coupang_id,
            "coupang_pw": coupang_pw,
            "brand_name": brand_name
        }
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            QMessageBox.information(self, "저장 완료", "설정이 저장되었습니다.")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "오류", f"저장 중 오류가 발생했습니다:\n{str(e)}")


class OrderApp(QMainWindow):
    # 크롤링 완료/에러 시그널
    crawlFinished = Signal(str)
    crawlError = Signal(str)
    progressUpdated = Signal(int)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("수강생 발주 프로그램")
        self.setFixedSize(650, 360)

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
        #      "invoice": …
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

        # ─── 5) 일괄 처리 / 주문서 생성 버튼 ─────────────────────────────────
        h5 = QHBoxLayout()
        self.btn_batch = QPushButton("일괄 처리")
        self.btn_batch.clicked.connect(self.run_batch_pipeline)
        self.btn_batch.setEnabled(False)

        self.btn_generate = QPushButton("주문서 생성")
        self.btn_generate.clicked.connect(self.generate_orders)
        self.btn_generate.setEnabled(False)

        h5.addWidget(self.btn_batch)
        h5.addWidget(self.btn_generate)

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
        """크롤링 완료 시 메인 스레드에서 처리"""
        self.progress.setVisible(False)
        QMessageBox.information(self, "완료", message)
        self.btn_generate.setEnabled(True)

        # 버튼 텍스트 및 슬롯 원복
        self.btn_batch.setText("일괄 처리")
        self.btn_batch.clicked.disconnect()
        self.btn_batch.clicked.connect(self.first_phase)
        self.btn_batch.setEnabled(False)

    @Slot(str)
    def on_crawl_error(self, errmsg):
        """크롤링 중 에러 발생 시 메인 스레드에서 처리"""
        self.progress.setVisible(False)
        QMessageBox.critical(self, "오류", errmsg)
        if self.driver:
            self.driver.quit()
            self.driver = None

        # 버튼 원복
        self.btn_batch.setText("일괄 처리")
        self.btn_batch.clicked.disconnect()
        self.btn_batch.clicked.connect(self.first_phase)
        self.btn_batch.setEnabled(True)

    def run_batch_pipeline(self):
        """일괄 처리 버튼 눌렀을 때 → 제로페이즈 → 퍼스트페이즈"""
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
        # 필터를 "Excel Files (*.xlsx *.xls)"로 지정하여 Excel만 선택 가능
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
        try:
            result = process_order_zip(self.order_zip_path)
            if result["failures"]:
                QMessageBox.warning(
                    self, "주의", "일부 파일 처리 실패:\n" + "\n".join(result["failures"])
                )
            else:
                QMessageBox.information(self, "완료", "ZIP 파일 처리 및 엑셀 생성 완료")
            return True
        except Exception as e:
            #QMessageBox.critical(self, "에러", f"ZIP 처리 중 오류:\n{str(e)}")
            print(self, "에러", f"ZIP 처리 중 오류:\n{str(e)}")
            return False
    
    def first_phase(self):
        """
        1) ZIP 해제 및 발주 데이터 파싱
           └─ ZIP 내부 한글 파일명 CP437 → CP949 디코딩
           └─ PO 헤더 영역에서 발주번호/센터명/입고예정일 추출
           └─ 아이템 테이블(헤더=19)에서
              • 인덱스1→ 상품명/상품코드, 인덱스2→ 바코드만 꺼내고 break
           └─ 추출된 (상품명, 바코드, 상품코드)를 콘솔에 디버그 출력
        """
        try:
            print("=== first_phase 시작 ===")

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
                raise Exception("ZIP 내부에 Excel 파일이 없습니다.")

            self.orders_data.clear()
            self.cached_shipment.clear()

            for idx, xlsx in enumerate(excel_files):
                # ── (가) 파일 전체를 헤더 없이 읽어들여 PO 헤더 & ETA 영역 찾기 ─────────────
                df_raw = pd.read_excel(xlsx, header=None, dtype=str)

                # ① “발주번호”가 있는 행을 찾아 PO번호 추출
                po_row = df_raw[
                    df_raw.iloc[:, 0].astype(str).str.contains("발주번호", na=False)
                ].index
                if len(po_row) == 0:
                    raise Exception(f"발주번호 행을 찾을 수 없습니다: {os.path.basename(xlsx)}")
                po_row = po_row[0]
                raw_po = df_raw.iloc[po_row, 2]
                po_no = str(raw_po).strip() if pd.notna(raw_po) else ""
                if not po_no:
                    raise Exception(f"발주번호가 비어 있습니다: {os.path.basename(xlsx)}")

                # ② “입고예정일시”가 있는 행을 찾아, 다음 행에서 ETA·센터명 추출
                eta_label_row = df_raw[
                    df_raw.iloc[:, 0].astype(str).str.contains("입고예정일시", na=False)
                ].index
                if len(eta_label_row) == 0:
                    raise Exception(f"입고예정일시 행을 찾을 수 없습니다: {os.path.basename(xlsx)}")
                eta_label_row = eta_label_row[0]
                data_row = eta_label_row + 1

                raw_eta = df_raw.iloc[data_row, 5]
                try:
                    eta = pd.to_datetime(raw_eta).to_pydatetime() if pd.notna(raw_eta) else None
                except:
                    eta = None

                raw_center = df_raw.iloc[data_row, 2]
                center = str(raw_center).strip() if pd.notna(raw_center) else ""

                # ── (나) 아이템 테이블 읽기 (헤더 = 19행 기준) ───────────────────────
                df_items = pd.read_excel(xlsx, header=19, dtype=str)
                df_items = df_items.loc[:, ~df_items.columns.str.startswith("Unnamed")]
                df_items.columns = df_items.columns.str.strip()

                print(f"파일: {os.path.basename(xlsx)} 아이템 헤더: {df_items.columns.tolist()}")
                
                """
                QMessageBox.information(
                    self, "아이템 헤더 확인",
                    f"파일: {os.path.basename(xlsx)}\n"
                    f"아이템 테이블 헤더: {df_items.columns.tolist()}"
                )
                """

                # “상품코드” 칼럼, “상품명/옵션/BARCODE” 칼럼 찾아두기
                col_product = next((c for c in df_items.columns if "상품코드" in c or "품번" in c), None)
                col_barcode = next((c for c in df_items.columns if "BARCODE" in c.upper()), None)

                if not col_product or not col_barcode:
                    raise Exception(
                        f"아이템 테이블에 '상품코드' 또는 '상품명/옵션/BARCODE' 칼럼이 없습니다。\n"
                        f"현재 칼럼: {df_items.columns.tolist()}"
                    )

                # ── (다) “첫 번째 블록(인덱스1→상품명/상품코드, 인덱스2→바코드)”만 읽고 break ─────────
                product_name = ""
                barcode = ""
                product_code = ""

                # 항상 인덱스 1에 상품명+상품코드, 인덱스 2에 바코드가 들어 있음
                if len(df_items) > 1:
                    # 인덱스 1에서 상품명·상품코드
                    raw_pc = df_items.iloc[1].get(col_product, "")
                    product_code = str(raw_pc).strip() if pd.notna(raw_pc) else ""

                    raw_pn = df_items.iloc[1].get(col_barcode, "")
                    product_name = str(raw_pn).strip() if pd.notna(raw_pn) else ""

                    # 인덱스 2에서 바코드
                    if len(df_items) > 2:
                        raw_bc2 = df_items.iloc[2].get(col_barcode, "")
                        barcode = str(raw_bc2).strip() if pd.notna(raw_bc2) else ""

                # 디버그 출력
                print(f"[디버그] PO {po_no} → product_name: '{product_name}', barcode: '{barcode}', product_code: '{product_code}'")

                # orders_data에 저장
                self.orders_data[po_no] = {
                    "barcode": barcode or "",
                    "product_code": product_code or "",
                    "product_name": product_name or "",
                    "center": center or "",
                    "eta": eta,
                    "shipment": None,
                    "invoice": f"{random.randint(1000000000, 9999999999)}"
                }

                percent = int((idx + 1) / len(excel_files) * 30)
                self.progressUpdated.emit(percent)

            # ── 파싱 완료 요약 출력 ───────────────────────────────────────────
            num_orders = len(self.orders_data)
            print(f"파싱 완료: 총 {num_orders}건의 발주 데이터를 읽었습니다.")
            for po_no, info in self.orders_data.items():
                print(f"   -> {po_no} | 바코드: '{info['barcode']}' | 상품명: '{info['product_name']}'")

            QMessageBox.information(self, "파싱 완료", f"총 {num_orders}건의 발주 데이터를 읽었습니다。\n(콘솔창을 확인하세요.)")
            if num_orders == 0:
                print(">>> orders_data가 비어있어 종료합니다.")
                return

            # ─── 2) Selenium WebDriver 생성 (디버깅용 로그/예외 처리) ─────────────
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
                print("!!! WebDriver 생성 오류:", e)
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

            # ─── 3) 자동 로그인 시도 (실패해도 수동 단계로) ─────────────────────────────
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
                    print("자동 로그인 실패, 수동 단계로 넘어갑니다。")
                    QMessageBox.information(self, "알림",
                                            "자동 로그인 시도 중 오류가 발생했습니다。\n"
                                            "수동으로 로그인 후 “로그인 완료” 버튼을 눌러주세요。")
            else:
                print("저장된 쿠팡 계정 정보가 없습니다。")
                QMessageBox.information(self, "알림",
                                        "쿠팡 아이디/비밀번호가 설정되어 있지 않습니다。\n"
                                        "브라우저에서 직접 로그인 완료 후 “로그인 완료” 버튼을 눌러주세요。")

            # ─── 4) “로그인 완료” 버튼 준비 ─────────────────────────────────────────
            self.btn_batch.setText("로그인 완료")
            self.btn_batch.clicked.disconnect()
            self.btn_batch.clicked.connect(self.second_phase)
            self.btn_batch.setEnabled(True)

        except Exception as e:
            print("!!! first_phase 예외 발생:", e)
            self.crawlError.emit(f"초기 처리 중 오류가 발생했습니다：\n{str(e)}")

    def second_phase(self):
        """
        “로그인 완료” 클릭 시:
        1) 메뉴 클릭(“Logistics”→“Shipments”) 후 발주번호 입력창 대기 → 크롤링 (진행 30~70)
        2) 발주확정 엑셀 생성 (진행 70~100)
        """
        self.btn_batch.setEnabled(False)
        self.progress.setVisible(True)

        threading.Thread(target=self.crawl_and_generate).start()

    def crawl_and_generate(self):
        """
        실제 크롤링 및 엑셀 생성 로직을 수행한 뒤,
        성공 시 crawlFinished.emit(msg), 실패 시 crawlError.emit(errmsg)
        """
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import os
            import time
            import shutil
            import getpass

            driver = self.driver
            self.progressUpdated.emit(30)

            try:
                btn_logistics = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/logistics']"))
                )
                btn_logistics.click()

                btn_shipments = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/ibs/asn/active']"))
                )
                btn_shipments.click()
            except Exception:
                raise Exception("메뉴 클릭 실패 (Logistics → Shipments)")

            try:
                search_input = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#purchaseOrderSeq"))
                )
            except:
                raise Exception("발주번호 입력창을 찾지 못했습니다.")

            download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            local_dir = os.path.join(os.getcwd(), "downloads")
            os.makedirs(local_dir, exist_ok=True)
            dir1 = os.path.join(local_dir, "LABEL")
            dir2 = os.path.join(local_dir, "MANIFEST")
            os.makedirs(dir1, exist_ok=True)
            os.makedirs(dir2, exist_ok=True)

            total = len(self.orders_data)
            for idx, (po_no, info) in enumerate(self.orders_data.items()):
                search_input.clear()
                search_input.send_keys(po_no)

                try:
                    btn_search = driver.find_element(By.CSS_SELECTOR, "button#shipment-search-btn")
                    btn_search.click()
                except:
                    raise Exception("검색 버튼 클릭 실패")

                try:
                    first_td = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table#parcel-tab tbody tr:first-child td:first-child"))
                    )
                    shipment_no = first_td.text.strip() if first_td.text else ""
                except:
                    shipment_no = ""

                center = info["center"]
                eta = info["eta"]
                key = f"{center}|{eta.strftime('%Y-%m-%d') if eta else ''}"
                self.cached_shipment[key] = shipment_no
                self.orders_data[po_no]["shipment"] = shipment_no or ""

                try:
                    if shipment_no:
                        driver.execute_script(f"window.open('https://supplier.coupang.com/ibs/shipment/parcel/pdf-label/generate?parcelShipmentSeq={shipment_no}', '_blank');")
                        time.sleep(1.5)
                        driver.execute_script(f"window.open('https://supplier.coupang.com/ibs/shipment/parcel/pdf-manifest/generate?parcelShipmentSeq={shipment_no}', '_blank');")
                        time.sleep(1.5)
                except Exception as e:
                    print("파일 다운로드 오류:", e)

                percent = 30 + int((idx + 1) / total * 40)
                self.progressUpdated.emit(percent)

            # 모든 다운로드 후 파일 정리
            time.sleep(5)

            for f in os.listdir(download_dir):
                path = os.path.join(download_dir, f)
                if os.path.isfile(path):
                    lower_f = f.lower()
                    if lower_f.startswith("shipment_label_document"):
                        shutil.move(path, os.path.join(dir1, f))
                    elif lower_f.startswith("shipment_manifest_document"):
                        shutil.move(path, os.path.join(dir2, f))

            driver.quit()
            self.driver = None

            self.progressUpdated.emit(100)
            self.crawlFinished.emit("발주확정 파일이 생성되었습니다.")

        except Exception as e:
            print("crawl_and_generate 예제 발생:", e)
            self.crawlError.emit(str(e))

    def wait_for_new_file(self, download_dir, before_files):
        for _ in range(30):
            time.sleep(0.5)
            after_files = set(os.listdir(download_dir))
            new_files = list(after_files - before_files)
            if new_files:
                file_path = os.path.join(download_dir, new_files[0])
                if not file_path.endswith(".crdownload"):
                    return file_path
        raise Exception("다운로드된 새 파일을 찾을 수 없습니다.")



    def generate_orders(self):
        """
        재고 엑셀("수량" 컬럼) 읽기 → 부족분 주문서/3PL 신청서 생성
        """
        try:
            # 재고 엑셀 읽기
            inv_df = pd.read_excel(self.inventory_xlsx_path, dtype=str)
            inv_df.fillna("0", inplace=True)  # 빈 값은 "0"으로 대체
            inventory_dict = {}
            for _, row in inv_df.iterrows():
                code = str(row.get("바코드", "") or row.get("SKU", "")).strip()
                try:
                    qty = int(float(row.get("수량", "0")))
                except:
                    qty = 0
                inventory_dict[code] = qty

            # 주문서(재고 부족분만)
            wb_order = Workbook()
            ws_order = wb_order.active
            ws_order.title = "주문서"
            headers_order = [
                "바코드명", "바코드", "상품코드", "센터명",
                "쉽쉽먼트번호", "발주번호", "입고예정일", "수량", "브랜드명"
            ]
            ws_order.append(headers_order)

            # 3PL 신청서(전 건)
            wb_3pl = Workbook()
            ws_3pl = wb_3pl.active
            ws_3pl.title = "3PL신청서"
            headers_3pl = [
                "쉽먼트번호", "바코드", "SKU(제품명)", "브랜드명",
                "수량", "입고예정일", "센터명"
            ]
            ws_3pl.append(headers_3pl)

            total = len(self.orders_data)
            for idx, (po_no, info) in enumerate(self.orders_data.items()):
                barcode = info["barcode"] or ""
                product_code = info["product_code"] or ""
                product_name = info["product_name"] or ""
                center = info["center"] or ""
                eta = info["eta"]
                shipment = info["shipment"] or ""
                brand = self.le_brand.text().strip() or ""

                stock_qty = inventory_dict.get(barcode, 0)
                order_qty = 1  # 필요 시 “발주 수량” 컬럼으로 대체

                # 재고 부족분만 주문서에 추가
                if stock_qty < order_qty:
                    need_qty = order_qty - stock_qty
                    row_order = [
                        product_name,  # 실제 상품명
                        barcode,
                        product_code,
                        center,
                        shipment,
                        po_no,
                        eta.strftime("%Y-%m-%d") if eta else "",
                        need_qty,
                        brand
                    ]
                    ws_order.append(row_order)

                # 3PL 신청서: 전 건 추가
                row_3pl = [
                    shipment,
                    barcode,
                    product_name,  # “SKU(제품명)” 칸에 상품명을 넣음
                    brand,
                    order_qty,
                    eta.strftime("%Y-%m-%d") if eta else "",
                    center
                ]
                ws_3pl.append(row_3pl)

                percent = int((idx + 1) / total * 100)
                self.progressUpdated.emit(percent)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            order_filename = f"주문서_{timestamp}.xlsx"
            pl3_filename = f"3PL신청서_{timestamp}.xlsx"
            wb_order.save(order_filename)
            wb_3pl.save(pl3_filename)

            QMessageBox.information(
                self, "완료",
                f"주문서와 3PL 신청서가 생성되었습니다。\n"
                f"- 주문서: {order_filename}\n"
                f"- 3PL 신청서: {pl3_filename}"
            )

        except Exception as e:
            QMessageBox.critical(self, "오류", f"주문서 생성 중 오류:\n{str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    check_version_or_exit()
    window = OrderApp()
    window.show()
    sys.exit(app.exec())
