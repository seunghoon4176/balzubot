

import openpyxl
import tkinter as tk
from tkinter import filedialog

def extract_products(filepath):
    wb = openpyxl.load_workbook(filepath)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    products = []
    for i in range(21, len(rows), 2):  # 0-based, 21번째(엑셀 22행)부터 2줄씩
        row1 = rows[i]
        row2 = rows[i+1] if i+1 < len(rows) else None
        if not row2:
            continue
        product_name = row1[2]
        product_code = row1[1]
        mfg_flag = row1[16]
        mfg_date = row1[17]
        barcode = row2[2]
        if barcode and str(barcode).startswith("R"):
            products.append({
                "상품명": product_name,
                "상품코드": product_code,
                "바코드": barcode,
                "제조일자관리": mfg_flag,
                "제조일자": mfg_date
            })
    return products

def select_and_extract():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="엑셀 파일 선택",
        filetypes=[("Excel files", "*.xlsx;*.xls")]
    )
    if file_path:
        products = extract_products(file_path)
        for p in products:
            print(p)
    else:
        print("파일이 선택되지 않았습니다.")

if __name__ == "__main__":
    select_and_extract()

