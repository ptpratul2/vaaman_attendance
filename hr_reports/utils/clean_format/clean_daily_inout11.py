# clean_daily_inout11.py
"""
Cleaner for Kakinada horizontal crystal-like report.

Key behaviour:
- Converts .xls -> .xlsx (kept mandatory)
- Detects period from first 5 lines (e.g. "Jul 01 2025 To Jul 31 2025")
- Detects the date row (01..31) (e.g. row index ~7)
- Detects repeating employee blocks where a row contains Emp. Code and Emp. Name
  (inline like "Emp. Code: V01118    Emp. Name: N MADHU BABU")
  (e.g. rows 11, 17, 23 ...). For each base row:
    - base: Emp. Code & Emp. Name
    - base+1: Status
    - base+2: InTime
    - base+3: OutTime
    - base+4: Total (Working Hours)
- Maps status tokens (including 0.5P and ½P) to canonical values
- Outputs the same columns used by other cleaners
"""

import os
import re
import tempfile
from datetime import datetime
from typing import Dict, Optional, Tuple, List

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
    print(f"[convert_xls_to_xlsx] Saved temporary .xlsx: {temp_xlsx}")
    return temp_xlsx


# -------------------------
# Helpers
# -------------------------
def parse_period_month(df: pd.DataFrame, max_rows: int = 5) -> datetime:
    for i in range(min(max_rows, len(df))):
        row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()])
        m = re.search(
            r'([A-Za-z]{3,9}\s+\d{1,2}\s+\d{4})\s+to\s+([A-Za-z]{3,9}\s+\d{1,2}\s+\d{4})',
            row_text,
            re.IGNORECASE,
        )
        if m:
            for fmt in ("%b %d %Y", "%B %d %Y"):
                try:
                    dt = datetime.strptime(m.group(1), fmt)
                    dt_first = datetime(year=dt.year, month=dt.month, day=1)
                    print(f"[parse_period_month] Found period: '{m.group(1)}' -> {dt_first:%Y-%m}")
                    return dt_first
                except Exception:
                    continue
    today = datetime.today()
    print(f"[parse_period_month] Period not found; using today: {today:%Y-%m}")
    return datetime(year=today.year, month=today.month, day=1)


def detect_date_row(df: pd.DataFrame, start: int = 0, max_check: int = 80) -> Optional[int]:
    day_pattern = re.compile(r'^\s*(0?[1-9]|[12]\d|3[01])(\s*[A-Za-z]*)?\s*$')
    for r in range(start, min(len(df), max_check)):
        vals = df.iloc[r].astype(str).fillna("").tolist()
        day_count = sum(1 for v in vals if day_pattern.match(v.strip()))
        if day_count >= 6:
            print(f"[detect_date_row] Day-row detected at index {r} (day_count={day_count})")
            return r
    print("[detect_date_row] Could not detect day row")
    return None


def build_date_map(date_row: pd.Series, month_dt: datetime) -> Dict[int, str]:
    date_map: Dict[int, str] = {}
    day_pattern = re.compile(r'^\s*(0?[1-9]|[12]\d|3[01])')
    for idx, cell in enumerate(date_row.tolist()):
        if pd.isna(cell):
            continue
        s = str(cell).strip()
        m = day_pattern.match(s)
        if m:
            try:
                day = int(m.group(1))
                dt = datetime(year=month_dt.year, month=month_dt.month, day=day)
                date_map[idx] = dt.strftime("%Y-%m-%d")
            except Exception:
                continue
    print(f"[build_date_map] Date map built with {len(date_map)} columns")
    return date_map


# -------------------------
# Employee row detection
# -------------------------
def detect_employee_row(df: pd.DataFrame, start: int = 0, max_check: int = 200) -> List[int]:
    """
    Detect rows like 'Emp. Code: V01118    Emp. Name: N MADHU BABU'
    Handles extra spaces, merged cells, and invisible characters.
    """
    emp_rows = []

    # Pattern now allows optional dot, colon, dash, and variable spacing
    pattern = re.compile(
        r"emp\.?\s*code\s*[:\-]?\s*[A-Za-z0-9]+\s+emp\.?\s*name\s*[:\-]?\s*[A-Za-z]",
        re.IGNORECASE,
    )

    print("\n[DEBUG] Checking first 50 rows for employee header match:\n")
    for r in range(start, min(len(df), max_check)):
        # Join all cell values into one clean string
        row_str = " ".join(str(x) for x in df.iloc[r].fillna("").tolist())
        clean_row = (
            row_str.replace("\xa0", " ")  # replace non-breaking spaces
            .replace("\t", " ")
            .strip()
        )
        clean_row = re.sub(r"\s+", " ", clean_row)  # normalize whitespace

        if pattern.search(clean_row):
            print(f"[EMP_MATCH] Row {r}: {clean_row}")
            emp_rows.append(r)

    if not emp_rows:
        raise ValueError(
            "No employee base rows found — could not detect lines like 'Emp. Code: ... Emp. Name: ...'."
        )

    print(f"[detect_employee_row] Found employee rows: {emp_rows}")
    return emp_rows




def parse_emp_code_name(row: pd.Series) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts 'Emp. Code' and 'Emp. Name' from a row.
    Handles cases where data may be in separate cells or have NaN fillers.
    """
    cells = [str(x).strip() for x in row.fillna("").tolist()]
    joined = " ".join(cells)
    joined = re.sub(r"\s+", " ", joined)

    # Try to match both fields even if Excel splits them oddly
    code = None
    name = None

    # Strategy 1: Use full-row regex
    m = re.search(
        r"Emp\.?\s*Code\s*[:\-]?\s*([A-Za-z0-9]+).*?Emp\.?\s*Name\s*[:\-]?\s*([A-Za-z][A-Za-z\s\.\-]+)",
        joined,
        re.IGNORECASE,
    )
    if m:
        code = m.group(1).strip()
        name = m.group(2).strip()
        return code, name

    # Strategy 2: cell-wise fallback
    for i, val in enumerate(cells):
        v = val.lower()
        if "emp" in v and "code" in v:
            # next non-empty cell may have the code
            for j in range(i, len(cells)):
                if re.match(r"^[A-Za-z0-9]+$", cells[j]):
                    code = cells[j]
                    break
        if "emp" in v and "name" in v:
            # join next few non-empty cells for name
            name_parts = []
            for j in range(i, len(cells)):
                if cells[j] and not re.search(r"emp", cells[j], re.I):
                    name_parts.append(cells[j])
            if name_parts:
                name = " ".join(name_parts)
            break

    return code, name


# -------------------------
# Formatting helpers
# -------------------------
def format_timestamp(date_str: str, time_val, is_checkin: bool) -> Optional[str]:
    if time_val is None or (isinstance(time_val, float) and pd.isna(time_val)) or str(time_val).strip() == "":
        return None
    try:
        if isinstance(time_val, (float, int)) and not isinstance(time_val, datetime):
            if 0.0 <= float(time_val) < 2.0:
                total_seconds = int(float(time_val) * 24 * 3600)
                hours = (total_seconds // 3600) % 24
                minutes = (total_seconds % 3600) // 60
                return f"{date_str} {hours:02d}:{minutes:02d}:00"
        t = str(time_val).strip()
        t = t.replace(".", ":")
        t = re.sub(r'[^\d:\sAPMapm]', '', t).strip()
        for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M%p"):
            try:
                dt = datetime.strptime(t, fmt)
                return f"{date_str} {dt.hour:02d}:{dt.minute:02d}:00"
            except Exception:
                continue
        parts = [p for p in t.split(":") if p != ""]
        if parts:
            h = int(parts[0]) if parts[0].isdigit() else (9 if is_checkin else 17)
            m = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            return f"{date_str} {h:02d}:{m:02d}:00"
    except Exception:
        pass
    return f"{date_str} {'09:00:00' if is_checkin else '17:00:00'}"


def map_status(raw_status) -> str:
    s = "" if pd.isna(raw_status) else str(raw_status).strip()
    s = s.replace("½", "0.5")
    mapping = {
       "P": "Present", "POW": "Present", "POH": "Present", "PWH": "Present", "WOP": "Present",
       "A": "Absent", "A1": "Absent",
       "WO": "Holiday", "H": "Holiday", 
       "CL": "On Leave", "PL": "On Leave", "SL": "On Leave", "EL": "On Leave", "RL": "On Leave", "LWP": "On Leave", "SDL": "On Leave", "QL": "On Leave", "TU": "On Leave", "CO": "On Leave", "TR": "On Leave", "OH": "On Leave", "ML": "On Leave", "CH": "On Leave", "SCL": "On Leave", "SPL": "On Leave",
       "MIS": "Half Day",  "HD": "Half Day", "HALF": "Half Day", "HLD": "Half Day",
       "WOH": "Work From Home","WFH": "Work From Home"
    }
    key = re.sub(r'[^A-Za-z0-9\.]', '', s).upper()
    return mapping.get(key, s)


def detect_shift(in_time: Optional[str], out_time: Optional[str]) -> str:
    def get_hour(ts: Optional[str]) -> Optional[int]:
        if not ts or str(ts).strip() == "" or pd.isna(ts):
            return None
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").hour
        except Exception:
            try:
                return datetime.strptime(ts, "%Y-%m-%d %H:%M").hour
            except Exception:
                return None

    in_hour = get_hour(in_time)
    out_hour = get_hour(out_time)
    hour = in_hour if in_hour is not None else out_hour
    if hour is None:
        return "G"
    if 6 <= hour < 14:
        return "A"
    if 14 <= hour < 22:
        return "B"
    if hour >= 22 or hour < 6:
        return "C"
    return "G"


# -------------------------
# Main cleaning function
# -------------------------
def clean_daily_inout11(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout11] Starting")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    working_file = input_path
    temp_created = False
    if input_path.lower().endswith(".xls"):
        working_file = convert_xls_to_xlsx(input_path)
        temp_created = True

    df = pd.read_excel(working_file, header=None, engine="openpyxl", dtype=object)
    print(f"[clean_daily_inout11] Raw shape: {df.shape}")

    month_dt = parse_period_month(df, max_rows=5)
    date_row_idx = detect_date_row(df, start=0, max_check=min(80, len(df)))
    if date_row_idx is None:
        date_row_idx = detect_date_row(df, start=0, max_check=len(df))
        if date_row_idx is None:
            raise ValueError("Could not locate the row containing day numbers (01..31).")

    date_map = build_date_map(df.iloc[date_row_idx], month_dt)
    if not date_map:
        raise ValueError("Could not map day columns from detected day row.")

    candidate_bases = detect_employee_row(df, start=date_row_idx + 1, max_check=len(df))

    records = []
    for base in candidate_bases:
        print(f"[clean_daily_inout11] Processing block at base row {base}")

        emp_row = df.iloc[base]
        status_row = df.iloc[base + 1] if base + 1 < len(df) else None
        in_row = df.iloc[base + 2] if base + 2 < len(df) else None
        out_row = df.iloc[base + 3] if base + 3 < len(df) else None
        total_row = df.iloc[base + 4] if base + 4 < len(df) else None

        emp_code, emp_name = parse_emp_code_name(emp_row)
        print(f"[clean_daily_inout11] Extracted Emp Code: {emp_code}, Emp Name: {emp_name}")
        if not emp_code and not emp_name:
            continue

        emp_doc_name = emp_code or ""

        for col_idx, date_str in date_map.items():
            st_val = status_row.iloc[col_idx] if status_row is not None and col_idx < len(status_row) else None
            in_val = in_row.iloc[col_idx] if in_row is not None and col_idx < len(in_row) else None
            out_val = out_row.iloc[col_idx] if out_row is not None and col_idx < len(out_row) else None
            tot_val = total_row.iloc[col_idx] if total_row is not None and col_idx < len(total_row) else None

            if all((pd.isna(x) or str(x).strip() == "") for x in (st_val, in_val, out_val, tot_val)):
                continue

            status_mapped = map_status(st_val)
            if status_mapped == "Holiday":
                continue

            in_ts = format_timestamp(date_str, in_val, True)
            out_ts = format_timestamp(date_str, out_val, False)
            shift_code = detect_shift(in_ts, out_ts)

            records.append({
                "Attendance Date": date_str,
                "Employee": emp_doc_name,
                "Employee Name": emp_name or "",
                "Status": status_mapped,
                "In Time": in_ts,
                "Out Time": out_ts,
                "Working Hours": "" if pd.isna(tot_val) else str(tot_val).strip(),
                "Over Time": "",
                "Shift": shift_code,
                "Company": company if company else "Vaaman Engineers India Limited",
                "Branch": branch if branch else "",
            })

    df_final = pd.DataFrame.from_records(records)

    if df_final.empty:
        raise ValueError("No attendance records parsed from Kakinada report.")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout11] Saved cleaned file: {output_path}")

    if temp_created and os.path.exists(working_file):
        try:
            os.unlink(working_file)
        except Exception:
            pass

    print("[clean_daily_inout11] Done ✅")
    return df_final
