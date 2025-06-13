# main.py – 2025-06-13 (상품정보 선검증 버전)
# ──────────────────────────────────────────────────────────────
# ① 프로그램 시작 → 상품정보.xlsx 존재 확인·템플릿 생성
# ② ZIP → 발주서 파싱 → 바코드 목록 확보
# ③ 상품정보와 비교해 누락 있으면 바로 중단
# ④ 통과 시에만 Selenium 크롤링 진행
# ⑤ 3PL 신청서 & 주문서(확정수량 그대로)
# ──────────────────────────────────────────────────────────────
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


# ─── 상수 ─────────────────────────────────────────────────────
CONFIG_FILE   = "config.json"
LOCAL_VERSION = "1.0.0"
VERSION_URL   = "https://seunghoon4176.github.io/balzubot/version.json"

PRODUCT_XLSX = os.path.join(os.path.dirname(__file__), "상품정보.xlsx")
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
    "1sYtryUcGjritwwU6IGxc49uGZAIzkICo2QNBVw0kyNo/export"
    "?format=csv&gid=794212207"
)

ICON_PATH = os.path.join(os.path.dirname(__file__), "images", "cashbot.ico")


def load_stock_df(biz_num: str) -> pd.DataFrame:
    """스프레드시트 → 사업자번호별 재고 DataFrame 반환"""
    df = pd.read_csv(STOCK_SHEET_CSV, dtype=str).fillna("")
    biz_col = next(c for c in df.columns if "사업자" in c)
    bc_col  = next(c for c in df.columns if "바코드" in c)
    qty_col = next(c for c in df.columns if "수량"   in c)

    df = df[df[biz_col].astype(str).str.strip() == biz_num]
    if df.empty:
        raise Exception("스프레드시트에 해당 사업자번호 재고가 없습니다.")
    return df[[bc_col, qty_col]].rename(
        columns={bc_col: "바코드", qty_col: "수량"}
    )

# ─── 버전 확인 ───────────────────────────────────────────────
def check_version_or_exit():
    try:
        r = requests.get(VERSION_URL, timeout=5)
        if r.status_code == 200 and r.json().get("version") != LOCAL_VERSION:
            QMessageBox.critical(
                None, "버전 만료",
                "현재 프로그램 버전이 만료되었습니다.\n최신 버전을 내려받아 주세요."
            )
            sys.exit(1)
    except Exception as e:
        QMessageBox.critical(None, "버전 확인 오류", str(e))
        sys.exit(1)


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

        # progress
        self.progress = QProgressBar(); self.progress.setRange(0, 100); self.progress.setVisible(False)

        for r in (row_zip, row_brand, row_set, row_run): lay.addLayout(r)
        lay.addWidget(self.progress)

        for w in (self.le_zip, self.le_brand): w.textChanged.connect(self._enable_run)

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
            # 1-A. ZIP 해제 및 발주서 파싱
            tmpdir = tempfile.mkdtemp(prefix="order_zip_")
            excel_files = []
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
                        if is_confirmed_excel(target):   # 확정본은 스킵
                            os.remove(target); continue
                        excel_files.append(target)

            if not excel_files:
                QMessageBox.information(self, "안내", "미확정 발주서가 없습니다.")
                return

            self.orders_data.clear()
            for idx, xlsx in enumerate(excel_files):
                df_raw = pd.read_excel(xlsx, header=None, dtype=str)
                po_row = df_raw[df_raw.iloc[:, 0].astype(str).str.contains("발주번호", na=False)].index[0]
                po_no  = str(df_raw.iloc[po_row, 2]).strip()

                eta_row = df_raw[df_raw.iloc[:, 0].astype(str).str.contains("입고예정일시", na=False)].index[0] + 1
                eta_raw = df_raw.iloc[eta_row, 5]
                eta     = pd.to_datetime(eta_raw, errors="coerce").to_pydatetime()

                center  = str(df_raw.iloc[eta_row, 2]).strip()

                df_items = pd.read_excel(xlsx, header=19, dtype=str)
                df_items = df_items.loc[:, ~df_items.columns.str.startswith("Unnamed")]
                df_items.columns = df_items.columns.str.strip()

                col_product = next((c for c in df_items.columns if "상품코드" in c or "품번" in c), None)
                col_barcode = next((c for c in df_items.columns if "BARCODE" in c.upper()), None)
                if not col_product or not col_barcode:
                    raise Exception(f"{os.path.basename(xlsx)}: '상품코드' 또는 'BARCODE' 열 없음")

                product_code = str(df_items.iloc[1][col_product]).strip()
                product_name = str(df_items.iloc[1][col_barcode]).strip()
                barcode      = str(df_items.iloc[2][col_barcode]).strip() if len(df_items) > 2 else ""

                self.orders_data[po_no] = {
                    "barcode":      barcode,
                    "product_code": product_code,
                    "product_name": product_name,
                    "center":       center,
                    "eta":          eta,
                    "shipment":     None,
                    "invoice":      str(random.randint(10**9, 10**10-1))
                }

                pct = int((idx + 1) / len(excel_files) * 30)
                self.progressUpdated.emit(pct)

            # 1-B. 상품정보.xlsx 바코드 선검증
            prod_df = pd.read_excel(PRODUCT_XLSX, dtype=str).fillna("")
            if "상품바코드" not in prod_df.columns:
                raise Exception("상품정보.xlsx에 '상품바코드' 열이 없습니다.")

            known_barcodes = set(prod_df["상품바코드"].astype(str).str.strip())
            needed_barcodes = {info["barcode"] for info in self.orders_data.values() if info["barcode"]}
            missing = [bc for bc in needed_barcodes if bc not in known_barcodes]

            if missing:
                QMessageBox.critical(
                    self, "누락 상품",
                    "다음 바코드가 상품정보에 없습니다:\n" + "\n".join(missing)
                )
                return   # Selenium 단계로 넘어가지 않고 종료

            try:
                inv_df = load_stock_df(self.business_number)
            except Exception as e:
                QMessageBox.critical(self, "재고 확인 오류", str(e))
                return

            stock_barcodes = set(inv_df["바코드"].astype(str).str.strip())
            missing_stock = [bc for bc in needed_barcodes if bc not in stock_barcodes]
            if missing_stock:
                QMessageBox.critical(
                    self, "누락 재고",
                    "다음 바코드가 재고 스프레드시트에 없습니다:\n" +
                    "\n".join(missing_stock)
                )
                return
            
            # 1-C. Selenium WebDriver 생성 & 로그인
            self.progress.setVisible(True)
            self.progressUpdated.emit(30)

            options = ChromeOptions(); options.add_argument("--start-maximized")
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

            if self.coupang_id and self.coupang_pw:  # 자동 로그인
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username']"))
                    ).send_keys(self.coupang_id)
                    self.driver.find_element(By.CSS_SELECTOR, "input[name='password']").send_keys(self.coupang_pw)
                    self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
                except Exception:
                    pass  # 자동 로그인 실패 → 수동 로그인

            # 버튼 전환
            self.btn_batch.setText("로그인 완료")
            self.btn_batch.clicked.disconnect()
            self.btn_batch.clicked.connect(self.second_phase)
            self.btn_batch.setEnabled(True)

        except Exception as e:
            print("!!! _first_phase 예외:", e)
            self.crawlError.emit(str(e))

    # ──────────────────────────────────────────────────────────
    # 2) Selenium 로그인 완료 후 크롤링
    # ──────────────────────────────────────────────────────────
    def second_phase(self):
        self.btn_batch.setEnabled(False)
        self.progress.setVisible(True)
        threading.Thread(target=self.crawl_and_generate).start()

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
            zip_path = shutil.make_archive(f"shipment_{ts}", "zip", target_dir)
            try: shutil.rmtree(target_dir)
            except Exception as e_del: print(f"[경고] shipment 폴더 삭제 실패: {e_del}")

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
        try:
            import openpyxl  # 원데이 템플릿 읽기에 사용

            # 1️⃣ 스프레드시트 재고 → dict
            inv_df = load_stock_df(self.business_number)
            inventory_dict = {
                str(r["바코드"]).strip(): int(float(r["수량"] or 0))
                for _, r in inv_df.iterrows()
            }
            used_stock: dict[str, int] = {}   # 바코드별 누적 소모량

            # 2️⃣ 상품정보.xlsx (원데이 주문서에 필요)
            prod_df = (
                pd.read_excel(PRODUCT_XLSX, dtype=str, engine="openpyxl")
                .fillna("")
            )

            # 3️⃣ 발주확정 엑셀
            confirm_path = os.path.join(os.getcwd(), "발주 확정 양식.xlsx")
            df_confirm = pd.read_excel(confirm_path, dtype=str, engine="openpyxl").fillna("")
            df_confirm["확정수량"] = (
                pd.to_numeric(df_confirm["확정수량"], errors="coerce").fillna(0).astype(int)
            )

            df_confirm["Shipment"] = df_confirm["발주번호"].map(
                lambda x: self.orders_data.get(str(x).strip(), {}).get("shipment", "")
            )
            df_confirm = df_confirm[df_confirm["확정수량"] > 0]

            # 4️⃣ 그룹화(Shipment·바코드 단위)
            group_cols = ["Shipment", "상품바코드", "상품이름", "물류센터", "입고예정일"]
            df_group = (
                df_confirm[group_cols + ["확정수량"]]
                .groupby(group_cols, as_index=False)["확정수량"]
                .sum()
            )

            # 5️⃣ 결과 워크북들
            wb_3pl, wb_order = Workbook(), Workbook()
            ws_3pl, ws_order = wb_3pl.active, wb_order.active
            ws_3pl.title, ws_order.title = "3PL신청서", "주문서"

            ws_3pl.append([
                "브랜드명", "쉽먼트번호", "발주번호", "SKU번호",
                "SKU(제품명)", "바코드", "수량", "", "입고예정일", "센터명"
            ])
            ws_order.append([
                "바코드명", "바코드", "상품코드", "쿠팡납품센터명",
                "쿠팡쉽먼트번호", "쿠팡입고예정일자",
                "입고마감준수여부", "발주 수량", "중국재고사용여부"
            ])

            wb_one = openpyxl.Workbook()
            ws_one = wb_one.active
            ws_one.title = "원데이주문서"

            # 헤더(1행) 직접 작성
            ws_one.append([
                "상품URL", "단가(위안)", "수량", "색상", "사이즈",
                "이미지URL", "상품바코드", "상품바코드명", "브랜드명"
            ])

            row_one = 2   # 데이터는 2행부터

            brand = self.le_brand.text().strip()

            # 6️⃣ 행 작성
            for _, r in df_group.iterrows():
                bc       = r["상품바코드"]
                pname    = r["상품이름"]
                center   = r["물류센터"]
                ship_no  = r["Shipment"]
                eta_raw  = r["입고예정일"]
                qty      = int(r["확정수량"])

                eta_str = pd.to_datetime(eta_raw, errors="coerce").strftime("%Y-%m-%d") if eta_raw else ""

                # 발주번호·상품코드 조회
                mask = (df_confirm["Shipment"] == ship_no) & (df_confirm["상품바코드"] == bc)
                po_no = product_code = ""
                if mask.any():
                    po_no        = str(df_confirm.loc[mask, "발주번호"].iloc[0]).strip()
                    product_code = str(df_confirm.loc[mask, "상품번호"].iloc[0]).strip()

                # → 3PL 신청서
                ws_3pl.append([
                    brand, ship_no, po_no, product_code,
                    pname, bc, qty, "", eta_str, center
                ])

                # 재고 차감
                already = used_stock.get(bc, 0)
                avail   = inventory_dict.get(bc, 0) - already
                need    = max(qty - max(avail, 0), 0)

                # → 기존 주문서: 부족분만
                if need > 0:
                    ws_order.append([
                        pname, bc, product_code, center,
                        ship_no, eta_str, "Y", need, "N"
                    ])

                # → 원데이 주문서: 부족분만
                if need > 0:
                    prod_row = prod_df[prod_df["상품바코드"] == bc]
                    prod_row = prod_row.iloc[0] if not prod_row.empty else {}

                    ws_one.cell(row_one, 1).value = prod_row.get("상품URL", "")          # 중국 사이트
                    ws_one.cell(row_one, 2).value = prod_row.get("상품단가(위안)", "")
                    ws_one.cell(row_one, 3).value = need                                # 수량
                    ws_one.cell(row_one, 4).value = ""                                  # 색상
                    ws_one.cell(row_one, 5).value = ""                                  # 사이즈
                    ws_one.cell(row_one, 6).value = prod_row.get("이미지URL", "")
                    ws_one.cell(row_one, 7).value = bc
                    ws_one.cell(row_one, 8).value = prod_row.get("상품바코드명", "")
                    ws_one.cell(row_one, 9).value = brand
                    row_one += 1

                used_stock[bc] = already + min(qty, max(avail, 0))

            # 7️⃣ 저장 및 알림
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            wb_3pl.save(f"3PL신청서_{ts}.xlsx")
            wb_order.save(f"주문서_{ts}.xlsx")
            wb_one.save(f"원데이주문서_{ts}.xlsx")

            QMessageBox.information(
                self, "완료",
                f"아래 3개 파일을 생성했습니다:\n"
                f"- 3PL신청서_{ts}.xlsx\n"
                f"- 주문서_{ts}.xlsx\n"
                f"- 원데이주문서_{ts}.xlsx"
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
    check_version_or_exit()
    win = OrderApp(); win.show()
    sys.exit(app.exec())
