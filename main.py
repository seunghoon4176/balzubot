# main.py – 2025-06-13 완전판
# ----------------------------------------------------------
# · 발주 ZIP → Selenium 크롤링
# · Google Sheets(재고리스트 탭) 사업자번호=A열 필터 CSV
# · 재고 차감 + 3PL 신청서 & 부족분 주문서
# ----------------------------------------------------------
import sys, os, json, zipfile, tempfile, random, threading, shutil, time, re, io, urllib.parse
from datetime import datetime

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

from order_processor import process_order_zip, is_confirmed_excel


# ──────────────────────────── 상수
CONFIG_FILE   = "config.json"
LOCAL_VERSION = "1.0.0"
VERSION_URL   = "https://seunghoon4176.github.io/balzubot/version.json"

SPREADSHEET_ID  = "1sYtryUcGjritwwU6IGxc49uGZAIzkICo2QNBVw0kyNo"
SPREADSHEET_GID = "794212207"       # 재고리스트 탭
ICON_PATH = os.path.join(os.path.dirname(__file__), "images", "cashbot.ico")


# ──────────────────────────── Google Sheets CSV
def fetch_inventory_for_biz(biz_no: str) -> pd.DataFrame:
    """
    탭 전체 CSV를 받아와 사업자번호 열(A열)에 대해 문자열·공백을
    무시하고 필터링한 DataFrame 반환.
    """
    url = (f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/"
           f"export?format=csv&gid={SPREADSHEET_GID}")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        # CSV → DataFrame (UTF-8 BOM 대응)
        df = pd.read_csv(io.BytesIO(r.content),
                         dtype=str,
                         encoding="utf-8-sig").fillna("")

        # A열 찾기: ‘사업자’가 포함된 첫 컬럼
        biz_col = next((c for c in df.columns if "사업자" in c), None)
        if biz_col is None:
            raise Exception("CSV에 '사업자번호' 열이 없습니다.")

        # 문자열로 변환 후 공백 제거 비교
        mask = df[biz_col].astype(str).str.strip() == str(biz_no).strip()
        return df[mask].reset_index(drop=True)
    except Exception as e:
        print("[WARN] CSV 다운로드/필터 실패:", e)
        return pd.DataFrame()


# ──────────────────────────── 버전 확인
def check_version_or_exit():
    try:
        r = requests.get(VERSION_URL, timeout=5)
        if r.status_code == 200 and r.json().get("version") != LOCAL_VERSION:
            QMessageBox.critical(None, "버전 만료",
                                 f"현재 버전({LOCAL_VERSION})은 만료되었습니다.\n"
                                 "새 버전을 내려받아 주세요.")
            sys.exit(1)
    except Exception as e:
        QMessageBox.critical(None, "버전 확인 오류", str(e)); sys.exit(1)


# ──────────────────────────── 설정 다이얼로그
class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("쿠팡 ID/PW 설정")
        self.setFixedSize(320, 250)
        self.setWindowIcon(QIcon(ICON_PATH))

        lay = QFormLayout(self)
        self.le_id   = QLineEdit(); lay.addRow("쿠팡 아이디:", self.le_id)
        self.le_pw   = QLineEdit(); self.le_pw.setEchoMode(QLineEdit.Password)
        lay.addRow("쿠팡 비밀번호:", self.le_pw)
        self.le_brand= QLineEdit(); lay.addRow("브랜드명:", self.le_brand)
        self.le_biz  = QLineEdit(); lay.addRow("사업자번호:", self.le_biz)

        btn = QPushButton("저장"); btn.clicked.connect(self._save); lay.addWidget(btn)
        self._load()

    def _load(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE,"r",encoding="utf-8") as f:
                d=json.load(f)
            self.le_id.setText(d.get("coupang_id",""))
            self.le_pw.setText(d.get("coupang_pw",""))
            self.le_brand.setText(d.get("brand_name",""))
            self.le_biz.setText(d.get("business_number",""))

    def _save(self):
        if not self.le_id.text().strip() or not self.le_pw.text().strip():
            QMessageBox.warning(self,"경고","쿠팡 ID/PW를 입력하세요."); return
        data = {
            "coupang_id": self.le_id.text().strip(),
            "coupang_pw": self.le_pw.text().strip(),
            "brand_name": self.le_brand.text().strip(),
            "business_number": self.le_biz.text().strip()
        }
        with open(CONFIG_FILE,"w",encoding="utf-8") as f:
            json.dump(data,f,ensure_ascii=False,indent=2)
        self.accept()


# ──────────────────────────── 메인 윈도우
class OrderApp(QMainWindow):

    crawlFinished   = Signal(str)
    crawlError      = Signal(str)
    progressUpdated = Signal(int)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("수강생 발주 프로그램")
        self.setFixedSize(680, 300)
        self.setWindowIcon(QIcon(ICON_PATH))

        # 설정 값
        self.order_zip_path = None
        self.coupang_id = self.coupang_pw = ""
        self.brand_name = self.business_number = ""

        # 런타임
        self.orders_data = {}; self.cached_shipment = {}; self.driver = None

        self._build_ui(); self._load_config()

        self.progressUpdated.connect(lambda v: self.progress.setValue(v))
        self.crawlFinished.connect(self._crawl_ok)
        self.crawlError.connect(self._crawl_err)

    # ── UI ──────────────────────────────────────────────────────
    def _build_ui(self):
        cen=QWidget(); self.setCentralWidget(cen); lay=QVBoxLayout(cen)

        # ZIP
        h1=QHBoxLayout(); h1.addWidget(QLabel("발주 ZIP:"))
        self.le_zip=QLineEdit(); self.le_zip.setReadOnly(True)
        btn_zip=QPushButton("파일 선택"); btn_zip.clicked.connect(self._pick_zip)
        h1.addWidget(self.le_zip); h1.addWidget(btn_zip)

        # 브랜드
        h2=QHBoxLayout(); h2.addWidget(QLabel("브랜드명:"))
        self.le_brand=QLineEdit(); h2.addWidget(self.le_brand)

        # 설정
        h3=QHBoxLayout(); h3.addStretch()
        btn_set=QPushButton("쿠팡 ID/PW 설정"); btn_set.clicked.connect(self._open_settings)
        h3.addWidget(btn_set)

        # 실행
        h4=QHBoxLayout()
        self.btn_run=QPushButton("일괄 처리"); self.btn_run.clicked.connect(self._run_pipeline)
        self.btn_run.setEnabled(False); h4.addWidget(self.btn_run)
        self.btn_batch = self.btn_run          # ★ 호환용 별칭

        # progress
        self.progress=QProgressBar(); self.progress.setRange(0,100); self.progress.setVisible(False)

        for h in (h1,h2,h3,h4): lay.addLayout(h)
        lay.addWidget(self.progress)

        for w in (self.le_zip, self.le_brand): w.textChanged.connect(self._enable_run)

    def _enable_run(self):
        self.btn_run.setEnabled(bool(self.le_zip.text() and self.le_brand.text() and self.business_number))

    # ── 설정 로드 ───────────────────────────────────────────────
    def _load_config(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE,"r",encoding="utf-8") as f:
                d=json.load(f)
            self.coupang_id=d.get("coupang_id",""); self.coupang_pw=d.get("coupang_pw","")
            self.brand_name=d.get("brand_name",""); self.business_number=d.get("business_number","")
            self.le_brand.setText(self.brand_name)
        self._enable_run()

    # ── UI slots ───────────────────────────────────────────────
    def _pick_zip(self):
        p,_=QFileDialog.getOpenFileName(self,"발주 ZIP 선택","","ZIP Files (*.zip)")
        if p: self.order_zip_path=p; self.le_zip.setText(p)

    def _open_settings(self):
        if SettingsDialog(self).exec()==QDialog.Accepted: self._load_config()

    # ── 파이프라인 엔트리 ───────────────────────────────────────
    def _run_pipeline(self):
        if self._zero_phase(): self.first_phase()

    # ------------------------------------------------------------------
    # 0) ZIP 선처리 (기존 order_processor 로직)
    def _zero_phase(self):
        try:
            res=process_order_zip(self.order_zip_path)
            if res["failures"]:
                QMessageBox.warning(self,"주의","일부 파일 처리 실패:\n"+"\n".join(res["failures"]))
            else:
                QMessageBox.information(self,"Zero Phase","ZIP 파일 처리 완료.")
            return True
        except Exception as e:
            print("Zero Phase 오류:",e); return False

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
                        if is_confirmed_excel(target_path):
                            print(f"[DEL ] 확정본 → {real_name}  (임시 사본 삭제)")
                            os.remove(target_path)          # ★★★ 여기 한 줄 ★★★
                            continue                        # excel_files 에 추가하지 않음
                        excel_files.append(target_path)

            if not excel_files:
                QMessageBox.information(self, "안내", "미확정 발주서가 없습니다.")
                return

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

            print("=== orders_data 최종 ===", list(self.orders_data.keys()))
            print("총 건수:", len(self.orders_data))

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
        """
        실제 크롤링 및 엑셀 생성 로직을 수행한 뒤,
        성공 시 crawlFinished.emit(msg), 실패 시 crawlError.emit(errmsg)
        """
        import re  # (함수 내부에 두면 상단 import 수정 없이도 동작)

        try:
            driver = self.driver
            self.progressUpdated.emit(30)

            driver.get("https://supplier.coupang.com/dashboard/KR")

            # ── 1) Logistics → Shipments 메뉴 진입 ────────────────────────
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

            # ── 2) 발주번호 입력창 확인 ───────────────────────────────────
            try:
                search_input = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#purchaseOrderSeq"))
                )
            except:
                raise Exception("발주번호 입력창을 찾지 못했습니다.")

            # ── 3) 다운로드·저장 폴더 지정 ────────────────────────────────
            download_dir = os.path.join(os.path.expanduser("~"), "Downloads")

            # 모든 PDF·엑셀을 한곳에 모을 최종 폴더
            target_dir = os.path.join(os.getcwd(), "shipment")
            os.makedirs(target_dir, exist_ok=True)

            # ── 4) 주문별 라벨·매니페스트 다운로드 ──────────────────────────
            total = len(self.orders_data)
            for idx, (po_no, info) in enumerate(self.orders_data.items()):
                # 검색
                search_input.clear()
                search_input.send_keys(po_no)
                try:
                    driver.find_element(By.CSS_SELECTOR, "button#shipment-search-btn").click()
                except:
                    raise Exception("검색 버튼 클릭 실패")

                # Shipment 번호 추출
                try:
                    first_td = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((
                            By.CSS_SELECTOR,
                            "table#parcel-tab tbody tr:first-child td:first-child"
                        ))
                    )
                    shipment_no = first_td.text.strip()
                except:
                    shipment_no = ""

                # 캐싱
                center = info["center"]
                eta    = info["eta"]
                key    = f"{center}|{eta.strftime('%Y-%m-%d') if eta else ''}"
                self.cached_shipment[key]      = shipment_no
                self.orders_data[po_no]["shipment"] = shipment_no

                # 다운로드
                if shipment_no:
                    try:
                        driver.execute_script(
                            f"window.open('https://supplier.coupang.com/ibs/shipment/parcel/"
                            f"pdf-label/generate?parcelShipmentSeq={shipment_no}', '_blank');"
                        )
                        time.sleep(1.5)
                        driver.execute_script(
                            f"window.open('https://supplier.coupang.com/ibs/shipment/parcel/"
                            f"pdf-manifest/generate?parcelShipmentSeq={shipment_no}', '_blank');"
                        )
                        time.sleep(1.5)
                    except Exception as e:
                        print(f"[경고] {shipment_no} 다운로드 중 오류: {e}")

                # 진행률
                percent = 30 + int((idx + 1) / total * 40)
                self.progressUpdated.emit(percent)

            # ── 5) 다운로드 완료 대기 후 파일 정리 ────────────────────────
            time.sleep(5)  # 네트워크 상태에 따라 조정

            dup_pat = re.compile(r"\s\(\d+\)(\.[^.]+)$")   # " (1).pdf", " (2).xlsx" …

            for fname in os.listdir(download_dir):
                low = fname.lower()
                if not (low.startswith("shipment_label_document") or
                        low.startswith("shipment_manifest_document")):
                    continue  # 다른 파일은 건드리지 않음

                src = os.path.join(download_dir, fname)

                # (1) 중복본은 삭제 (뒤에 " (1)", " (2)" 붙은 파일)
                if dup_pat.search(fname):
                    try:
                        os.remove(src)
                    except FileNotFoundError:
                        pass
                    continue

                # (2) 원본은 ./shipment 로 이동
                shutil.move(src, os.path.join(target_dir, fname))

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")          # ① 타임스탬프 생성
            zip_path = shutil.make_archive(f"shipment_{ts}", "zip", target_dir) 
            try:
                shutil.rmtree(target_dir)
            except Exception as e_del:
                print(f"[경고] shipment 폴더 삭제 실패: {e_del}")

            # ── 6) 마무리 ────────────────────────────────────────────────
            driver.quit()
            self.driver = None

            self.progressUpdated.emit(100)
            self.crawlFinished.emit("발주확정 파일(라벨·매니페스트)이 모두 생성되었습니다.")

        except Exception as e:
            print("crawl_and_generate 예외 발생:", e)
            self.crawlError.emit(str(e))

    # ── 크롤 완료/오류 핸들러 ────────────────────────────────────
    def _crawl_ok(self,msg):
        self.progress.setVisible(False); QMessageBox.information(self,"완료",msg)
        try: self._generate_orders()
        except Exception as e: QMessageBox.critical(self,"주문서 오류",str(e))
        self._reset_btn()

    def _crawl_err(self,msg):
        self.progress.setVisible(False); QMessageBox.critical(self,"크롤 오류",msg)
        if self.driver: self.driver.quit(); self.driver=None
        self._reset_btn()

    def _reset_btn(self):
        self.btn_run.setText("일괄 처리")
        self.btn_run.clicked.disconnect(); self.btn_run.clicked.connect(self._run_pipeline)
        self.btn_run.setEnabled(True)

    # ── 주문서·3PL 생성 ───────────────────────────────────────
    def _generate_orders(self):
        inv_df = fetch_inventory_for_biz(self.business_number)
        if inv_df.empty:
            raise Exception("스프레드시트에 해당 사업자번호 재고가 없습니다.")

        inventory = {
            str(r["바코드"]).strip(): int(float(r.get("수량",0) or 0))
            for _, r in inv_df.iterrows()
            if str(r.get("바코드","")).strip()
        }

        confirm_path=os.path.join(os.getcwd(),"발주 확정 양식.xlsx")
        df_confirm=pd.read_excel(confirm_path,dtype=str).fillna("")
        df_confirm=df_confirm[df_confirm["발주번호"].isin(self.orders_data.keys())]
        df_confirm["확정수량"]=pd.to_numeric(df_confirm["확정수량"],errors="coerce").fillna(0).astype(int)
        df_confirm["Shipment"]=df_confirm["발주번호"].map(
            lambda x:self.orders_data.get(str(x).strip(),{}).get("shipment",""))
        df_confirm=df_confirm[df_confirm["확정수량"]>0]

        grp_cols=["Shipment","상품바코드","상품이름","물류센터","입고예정일"]
        df_grp=df_confirm[grp_cols+["확정수량"]].groupby(grp_cols,as_index=False)["확정수량"].sum()

        wb3, wbo = Workbook(), Workbook(); ws3, wso = wb3.active, wbo.active
        ws3.title="3PL신청서"; wso.title="주문서"
        ws3.append(["브랜드명","쉽먼트번호","발주번호","SKU번호","SKU(제품명)","바코드","수량","공란","입고예정일","센터명"])
        wso.append(["바코드명","바코드","상품코드","쿠팡납품센터명","쿠팡쉽먼트번호","쿠팡입고예정일자",
                    "입고마감준수여부","발주 수량","중국재고사용여부"])

        used={}
        for _,r in df_grp.iterrows():
            bc=str(r["상품바코드"]).strip(); pname=str(r["상품이름"]).strip()
            ctr=str(r["물류센터"]).strip(); shp=r["Shipment"]; qty=int(r["확정수량"])
            try: eta_str=pd.to_datetime(r["입고예정일"]).strftime("%Y-%m-%d")
            except: eta_str=""
            mask=(df_confirm["Shipment"]==shp)&(df_confirm["상품바코드"]==bc)
            po=sku=""
            if mask.any():
                po=str(df_confirm.loc[mask,"발주번호"].iloc[0]).strip()
                sku=str(df_confirm.loc[mask,"상품번호"].iloc[0]).strip()
            ws3.append([self.brand_name,shp,po,sku,pname,bc,qty,"",eta_str,ctr])

            avail=inventory.get(bc,0)-used.get(bc,0); need=max(qty-max(avail,0),0)
            if need>0:
                wso.append([pname,bc,sku,ctr,shp,eta_str,"Y",need,"N"])
            used[bc]=used.get(bc,0)+qty

        ts=datetime.now().strftime("%Y%m%d_%H%M%S")
        wb3.save(f"3PL신청서_{ts}.xlsx"); wbo.save(f"주문서_{ts}.xlsx")
        QMessageBox.information(self,"완료",
            f"3PL신청서_{ts}.xlsx / 주문서_{ts}.xlsx 생성 완료")


# ─── main ───────────────────────────────────────────────────
if __name__ == "__main__":
    app=QApplication(sys.argv)
    check_version_or_exit()
    win=OrderApp(); win.show()
    sys.exit(app.exec())
