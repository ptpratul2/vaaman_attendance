# clean_crystal_excel.py
import os
import re
import tempfile
from datetime import datetime
from typing import List, Dict, Optional
import frappe

import pandas as pd
import xlrd
from openpyxl import Workbook

# -------------------------
# .xls -> .xlsx conversion
# -------------------------
def convert_xls_to_xlsx(xls_path: str) -> str:
    print(f"[convert_xls_to_xlsx] Converting .xls to .xlsx: {xls_path}")
    book = xlrd.open_workbook(xls_path, formatting_info=False)
    sheet = book.sheet_by_index(0)
    print(f"[convert_xls_to_xlsx] Original .xls dimensions: {sheet.nrows} rows x {sheet.ncols} cols")

    wb = Workbook()
    ws = wb.active

    for r in range(sheet.nrows):
        for c in range(sheet.ncols):
            cell_value = sheet.cell_value(r, c)
            if sheet.cell_type(r, c) == xlrd.XL_CELL_DATE:
                try:
                    cell_value = xlrd.xldate_as_datetime(cell_value, book.datemode)
                except Exception:
                    pass
            ws.cell(row=r + 1, column=c + 1).value = cell_value

    temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
    wb.save(temp_xlsx)
    print(f"[convert_xls_to_xlsx] Saved temporary .xlsx file: {temp_xlsx}")
    return temp_xlsx

# -------------------------
# Helpers
# -------------------------
def find_report_range_row(df: pd.DataFrame, max_rows: int = 5) -> Optional[int]:
    month_pattern = re.compile(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b', re.IGNORECASE)
    for i in range(min(max_rows, len(df))):
        row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()]).strip()
        if "to" in row_text.lower() and month_pattern.search(row_text):
            return i
    return None

def parse_month_year_from_range(text: str) -> Optional[datetime]:
    m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s*\d{1,2}\,?\s*\d{4}', text, re.IGNORECASE)
    if m:
        try:
            return datetime.strptime(m.group(0).replace('.', ''), "%b %d %Y")
        except Exception:
            pass
    m2 = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s*\d{4}', text, re.IGNORECASE)
    if m2:
        try:
            return datetime.strptime(m2.group(0).replace('.', ''), "%b %Y")
        except Exception:
            pass
    return None

def detect_date_row(df: pd.DataFrame, start_search: int = 0, max_rows: int = 20) -> Optional[int]:
    for r in range(start_search, min(len(df), max_rows)):
        row = df.iloc[r].astype(str).fillna("").tolist()
        day_count = sum(1 for cell in row if re.match(r'^\s*\d{1,2}\b', cell))
        if day_count >= 6:
            print(f"[detect_date_row] Found likely date row at index {r} (day_count={day_count})")
            return r
    print("[detect_date_row] Could not find date row by heuristic")
    return None

def build_date_map(date_row: List[object], month_dt: datetime) -> Dict[int, str]:
    date_map = {}
    for idx, cell in enumerate(date_row):
        if pd.isna(cell):
            continue
        s = str(cell).strip()
        m = re.match(r'^\s*(\d{1,2})\b', s)
        if m:
            day = int(m.group(1))
            try:
                composed = datetime(year=month_dt.year, month=month_dt.month, day=day)
                date_map[idx] = composed.strftime("%Y-%m-%d")  # <-- dash format
            except Exception:
                continue
    print(f"[build_date_map] Built date_map for {len(date_map)} day columns")
    return date_map

def format_timestamp(date_str: str, time_val, is_checkin=True):
    """Combine date and time into 'YYYY-MM-DD HH:MM:00' 24-hour format."""
    if pd.isna(time_val) or str(time_val).strip() == "":
        return None

    # Try to parse numeric Excel time
    if isinstance(time_val, (float, int)):
        total_seconds = int(float(time_val) * 24 * 3600)
        hours = (total_seconds // 3600) % 24
        minutes = (total_seconds % 3600) // 60
    else:
        # String like "9.28" or "09:28" or "9:28 AM"
        t_str = str(time_val).strip().replace('.', ':')
        try:
            # Try 24-hour first
            parsed = datetime.strptime(t_str, "%H:%M")
            hours = parsed.hour
            minutes = parsed.minute
        except ValueError:
            try:
                # Try 12-hour with AM/PM
                parsed = datetime.strptime(t_str, "%I:%M %p")
                hours = parsed.hour
                minutes = parsed.minute
            except ValueError:
                # Fallback: split by :
                parts = re.split(r'[:]', t_str)
                hours = int(parts[0]) if parts[0].isdigit() else 0
                minutes = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

    # Default if parsing failed completely
    if hours == 0 and minutes == 0:
        hours = 9 if is_checkin else 17  # 9:00 for in, 17:00 for out

    return f"{date_str} {hours:02d}:{minutes:02d}:00"

# -------------------------
# Main function
# -------------------------
def clean_crystal_excel(input_path: str, output_path: str, company: str = None, branch: str = None,
                        header_keywords: List[str] = None, min_nonempty_ratio: float = 0.35) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_crystal_excel] Starting")
    print(f"[clean_crystal_excel] Input: {input_path}")
    print(f"[clean_crystal_excel] Output: {output_path}")
    print(f"[clean_crystal_excel] Company: {company}")
    print(f"[clean_crystal_excel] Branch: {branch}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    working_file = input_path
    temp_created = False
    if input_path.lower().endswith(".xls"):
        working_file = convert_xls_to_xlsx(input_path)
        temp_created = True

    df_raw = pd.read_excel(working_file, header=None, engine="openpyxl", dtype=object)
    print(f"[clean_crystal_excel] Loaded raw DataFrame shape: {df_raw.shape}")

    range_row_idx = find_report_range_row(df_raw, max_rows=6)
    month_dt = None
    if range_row_idx is not None:
        range_text = " ".join(df_raw.iloc[range_row_idx].dropna().astype(str).tolist())
        month_dt = parse_month_year_from_range(range_text)
        print(f"[clean_crystal_excel] Found range row {range_row_idx}: '{range_text}' -> month_dt={month_dt}")
    else:
        print("[clean_crystal_excel] WARNING: report range row not found; will use current month")
        month_dt = datetime.today()

    search_start = range_row_idx + 1 if range_row_idx is not None else 0
    date_row_idx = detect_date_row(df_raw, start_search=search_start, max_rows=40)
    if date_row_idx is None:
        if 5 < len(df_raw):
            print("[clean_crystal_excel] Trying fallback row index 5 for date row")
            date_row_idx = 5
        else:
            raise ValueError("Could not detect date row containing day labels")

    date_row = df_raw.iloc[date_row_idx].tolist()
    date_map = build_date_map(date_row, month_dt)

    overtime_col_idx = None
    search_rows_for_header = list(range(max(0, date_row_idx - 2), min(len(df_raw), date_row_idx + 6)))
    for r in search_rows_for_header:
        for c, cell in enumerate(df_raw.iloc[r].astype(str).fillna("")):
            if re.search(r'\bover\s*time\b|\bovertime\b|\bOT\b', str(cell), re.IGNORECASE):
                overtime_col_idx = c
                print(f"[clean_crystal_excel] Found overtime header at row {r}, col {c}")
                break
        if overtime_col_idx is not None:
            break

    records = []
    r = date_row_idx + 1
    total_rows = len(df_raw)

    status_map = {
        "H": "Holiday",
        "WO": "Present",
        "A": "Absent",
        "P": "Present",
        "½P": "Half Day",
        "0.5P": "Half Day",
        ".5P": "Half Day",
        "HD": "Half Day",
        "HALF": "Half Day",
        "H½P": "Half Day",
        "½BH": "Half Day",
        "T": "Terminated",
        "ML": "On Leave",
        "CO": "On Leave",
        "HP": "Half Day",
    }

    # Statuses to exclude from records
    excluded_statuses = ["Holiday", "Terminated"]

    while r < total_rows:
        row0 = df_raw.iloc[r].astype(object).tolist()

        emp_code_label_col = None
        for c, cell in enumerate(row0):
            if isinstance(cell, str) and re.match(r'^\s*Emp\.?\s*Code', cell, re.IGNORECASE):
                emp_code_label_col = c
                break

        if emp_code_label_col is None:
            r += 1
            continue

        emp_code = None
        for cc in range(emp_code_label_col + 1, len(row0)):
            candidate = row0[cc]
            if pd.notna(candidate) and str(candidate).strip() != "":
                emp_code = str(candidate).strip()
                break

        emp_name = None
        for c, cell in enumerate(row0):
            if isinstance(cell, str) and re.match(r'^\s*Emp\.?\s*Name', cell, re.IGNORECASE):
                for cc in range(c + 1, len(row0)):
                    candidate = row0[cc]
                    if pd.notna(candidate) and str(candidate).strip() != "":
                        emp_name = str(candidate).strip()
                        break
                break

        if not emp_name:
            candidates = [(c_idx, str(val).strip()) for c_idx, val in enumerate(row0)
                          if pd.notna(val) and isinstance(val, str) and len(str(val).strip()) > 3]
            if candidates:
                for c_idx, txt in candidates:
                    if not re.match(r'^(Days|Status|InTime|OutTime|Total)$', txt, re.IGNORECASE) and not re.match(r'Emp', txt, re.IGNORECASE):
                        emp_name = txt
                        break

        status_row = None
        intime_row = None
        outtime_row = None
        totals_row = None
        search_end = min(total_rows, r + 20)  # Increased from 12 for more robustness
        for r2 in range(r + 1, search_end):
            row_vals = df_raw.iloc[r2].astype(object).tolist()
            first_texts = [str(x).strip() if pd.notna(x) else "" for x in row_vals[:6]]
            if any(re.match(r'^\s*Status\s*$', t, re.IGNORECASE) for t in first_texts):
                status_row = df_raw.iloc[r2]
            if any(re.match(r'^\s*InTime\s*$', t, re.IGNORECASE) for t in first_texts):
                intime_row = df_raw.iloc[r2]
            if any(re.match(r'^\s*OutTime\s*$', t, re.IGNORECASE) for t in first_texts):
                outtime_row = df_raw.iloc[r2]
            if any(re.match(r'^\s*Total\s*$', t, re.IGNORECASE) for t in first_texts):
                totals_row = df_raw.iloc[r2]
            if status_row is not None and intime_row is not None and outtime_row is not None:
                break

        if status_row is None:
            r += 1
            continue

        for col_idx, date_str in date_map.items():
            raw_status = status_row.iloc[col_idx] if col_idx < len(status_row) else None
            if pd.isna(raw_status) or str(raw_status).strip() == "":
                continue
            raw_status_s = str(raw_status).strip()
            status_final = status_map.get(raw_status_s, raw_status_s)

            # Skip records with excluded statuses
            if status_final in excluded_statuses:
                print(f"[clean_crystal_excel] Skipping {status_final} record for {emp_name} on {date_str}")
                continue

            check_in = None
            if intime_row is not None and col_idx < len(intime_row):
                check_in = format_timestamp(date_str, intime_row.iloc[col_idx], is_checkin=True)

            check_out = None
            if outtime_row is not None and col_idx < len(outtime_row):
                check_out = format_timestamp(date_str, outtime_row.iloc[col_idx], is_checkin=False)

            overtime_val = None
            if overtime_col_idx is not None and totals_row is not None and overtime_col_idx < len(totals_row):
                ot_cand = totals_row.iloc[overtime_col_idx]
                if pd.notna(ot_cand) and str(ot_cand).strip() != "":
                    overtime_val = ot_cand
            if overtime_val is None and totals_row is None and overtime_col_idx is not None:
                for try_r in range(r + 1, search_end):
                    try_row = df_raw.iloc[try_r]
                    if overtime_col_idx < len(try_row):
                        ot_cand = try_row.iloc[overtime_col_idx]
                        if pd.notna(ot_cand) and str(ot_cand).strip() != "":
                            overtime_val = ot_cand
                            break

            # Use ERPNext Attendance field labels as columns
            rec = {
                "Attendance Date": date_str,
                "Employee": emp_code if emp_code else "",  # Assuming employee code is the Employee ID
                "Employee Name": emp_name if emp_name else "",
                "Status": status_final,
                "In Time": check_in,
                "Out Time": check_out,
                "Company": company if company else "Vaaman Engineers India Limited",
                "Branch": branch if branch else "",
                "Over Time": overtime_val  # Assuming custom field; remove if not present
            }
            records.append(rec)

        found_next_emp = False
        for look_r in range(r + 1, min(total_rows, r + 20)):
            rowlook = df_raw.iloc[look_r].astype(str).fillna("").tolist()
            if any(re.match(r'^\s*Emp\.?\s*Code', str(x), re.IGNORECASE) for x in rowlook[:6]):
                r = look_r
                found_next_emp = True
                break
        if not found_next_emp:
            r += 1

    # Use ERPNext Attendance field labels as columns
    final_cols = ["Attendance Date", "Employee", "Employee Name", "Status", "In Time", "Out Time", "Company", "Branch", "Over Time"]
    df_final = pd.DataFrame.from_records(records, columns=final_cols)
  
    # Remove any rows where all critical fields are empty
    df_final = df_final.dropna(subset=["Attendance Date", "Employee", "Status"], how="all")
  
    print(f"[clean_crystal_excel] Built final DataFrame with {len(df_final)} rows (after filtering out Holiday/Terminated)")

    if df_final.empty:
        raise ValueError("No attendance records parsed from the report. Check input file format.")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_crystal_excel] Saved output to: {output_path}")

    if temp_created and os.path.exists(working_file):
        try:
            os.unlink(working_file)
            print(f"[clean_crystal_excel] Removed temporary file: {working_file}")
        except Exception:
            pass

    print("[clean_crystal_excel] Done ✅")
    return df_final