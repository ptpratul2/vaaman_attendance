# hr_reports/utils/clean_format/clean_daily_inout29.py
# =====================================================
# Cleaning Script for Daily In-Out (Vertical Report)
# Detects header row dynamically (within first 15–20 rows)
# =====================================================

import os
import pandas as pd
import frappe
from datetime import datetime
from typing import Optional


# =========================
#  .xls -> .xlsx conversion
# =========================
def convert_xls_to_xlsx(xls_path: str) -> str:
    """Convert .xls file to .xlsx using openpyxl (for consistent parsing)."""
    import xlrd
    from openpyxl import Workbook
    import tempfile

    print(f"[convert_xls_to_xlsx] Converting .xls to .xlsx: {xls_path}")
    book = xlrd.open_workbook(xls_path, formatting_info=False)
    sheet = book.sheet_by_index(0)
    wb = Workbook()
    ws = wb.active
    for r in range(sheet.nrows):
        for c in range(sheet.ncols):
            val = sheet.cell_value(r, c)
            if sheet.cell_type(r, c) == xlrd.XL_CELL_DATE:
                try:
                    val = xlrd.xldate_as_datetime(val, book.datemode)
                except Exception:
                    pass
            ws.cell(row=r + 1, column=c + 1).value = val

    tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
    wb.save(tmp_path)
    print(f"[convert_xls_to_xlsx] Saved temporary .xlsx: {tmp_path}")
    return tmp_path


# =========================
#  Helper Functions
# =========================
def map_status(raw_status) -> str:
    s = "" if pd.isna(raw_status) else str(raw_status).strip()
    mapping = {
        "P": "Present", "POW": "Present", "POH": "Present", "PWH": "Present", "RL": "Present", "TU": "Present", "QL": "Present",
        "A": "Absent", "A1": "Absent",
        "WO": "Holiday", "H": "Holiday",
        "CL": "On Leave", "PL": "On Leave", "SL": "On Leave", "EL": "On Leave",
        "LWP": "On Leave", "SDL": "On Leave",
        "CO": "On Leave", "TR": "On Leave", "OH": "On Leave", "SP": "On Leave",
        "ML": "On Leave", "CH": "On Leave", "SCL": "On Leave", "SPL": "On Leave",
        "MIS": "Half Day", "HD": "Half Day", "HALF": "Half Day", "HLD": "Half Day",
        "WOH": "Work From Home", "WFH": "Work From Home",
    }
    return mapping.get(s, s if s else "Absent")


def format_datetime(date_val, time_val):
    if pd.isna(date_val) or pd.isna(time_val):
        return None

    # Convert date_val to datetime
    if not isinstance(date_val, (datetime, pd.Timestamp)):
        date_val = pd.to_datetime(date_val, errors="coerce")
    if pd.isna(date_val):
        return None

    # Handle time_val
    hours = minutes = seconds = 0
    try:
        if isinstance(time_val, (datetime, pd.Timestamp)):
            hours = time_val.hour
            minutes = time_val.minute
            seconds = time_val.second
        elif isinstance(time_val, (float, int)):
            # Excel time serial -> seconds
            total_seconds = int(time_val * 24 * 3600)
            hours = (total_seconds // 3600) % 24
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
        else:
            # string like "05:49 AM"
            t_str = str(time_val).strip()
            if not t_str or t_str.lower() in ["nan", "none"]:
                return None
            t_obj = pd.to_datetime(t_str).time()
            hours = t_obj.hour
            minutes = t_obj.minute
            seconds = t_obj.second
    except Exception:
        return None

    # Keep in 12-hour format if needed
    suffix = "AM"
    if hours >= 12:
        suffix = "PM"
        if hours > 12:
            hours -= 12
    elif hours == 0:
        hours = 12

    return f"{date_val.strftime('%Y-%m-%d')} {hours:02d}:{minutes:02d}:{seconds:02d} {suffix}"



def _calculate_overtime(ot_val) -> str:
    """Calculate OT: return float value only if > 0, else empty string."""
    try:
        ot_float = float(ot_val)
    except Exception:
        ot_float = 0.0
    return ot_float if ot_float > 0 else ""


def _clean_shift_value(shift_val: Optional[str]) -> str:
    """Extract only the first capital letter from Shift column (e.g., 'Shift A' -> 'A')."""
    if not shift_val or pd.isna(shift_val):
        return ""
    s = str(shift_val).strip()
    if s.lower().startswith("shift"):
        s = s.replace("Shift", "").strip()
    return s[0].upper() if s else ""


def _detect_header_row(excel_path: str, required_cols: list, max_rows: int = 20) -> int:
    """Find the header row index (0-based) by scanning top N rows."""
    temp_df = pd.read_excel(excel_path, header=None, nrows=max_rows)
    for i in range(len(temp_df)):
        row_values = [str(x).strip() for x in temp_df.iloc[i].tolist()]
        match_count = sum(any(req.lower() == val.lower() for val in row_values) for req in required_cols)
        if match_count >= 5:  # at least 5 out of 9 matches = likely header row
            print(f"[Header Detection] Found header row at index {i}: {row_values}")
            return i
    raise ValueError(f"Could not find header row within first {max_rows} rows")


# =========================
#  Main Cleaning Function
# =========================
def clean_daily_inout29(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    """
    Clean Attendance Report (Vertical layout)
    Detects header dynamically, maps and normalizes key fields for ERPNext import.
    """
    print("=" * 80)
    print("[clean_daily_inout29] Starting")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Company: {company}")
    print(f"Branch: {branch}")
    print("=" * 80)

    # Convert .xls if needed
    working_file = input_path
    temp_created = False
    if input_path.lower().endswith(".xls"):
        working_file = convert_xls_to_xlsx(input_path)
        temp_created = True

    required_cols = ["Date", "Workmen", "IDNo", "In Time", "Out Time", "Man Hrs", "OT", "Status", "Shift"]

    # --- detect header row dynamically
    header_row = _detect_header_row(working_file, required_cols, max_rows=20)

    # --- read file again from detected header
    df_raw = pd.read_excel(working_file, engine="openpyxl", header=header_row)
    print(f"[clean_daily_inout29] Loaded DataFrame with header at row {header_row}, shape={df_raw.shape}")

    # sanity check
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing required columns even after header detection: {missing}")

    records = []
    for _, row in df_raw.iterrows():
        att_date = row.get("Date")
        gp_no = row.get("IDNo") if pd.notna(row.get("IDNo")) else ""
        emp_name = str(row.get("Workmen")).strip() if pd.notna(row.get("Workmen")) else ""
        status = map_status(row.get("Status"))
        in_time = format_datetime(att_date, row.get("In Time"))
        out_time = format_datetime(att_date, row.get("Out Time"))
        shift = _clean_shift_value(row.get("Shift"))
        work_hrs = row.get("Man Hrs")
        over_time = _calculate_overtime(row.get("OT"))

        # frappe lookup
        try:
            emp_code = frappe.db.get_value("Employee", {"attendance_device_id": gp_no}, "name") or ""
        except Exception:
            emp_code = ""

        if not gp_no and not emp_name and pd.isna(att_date):
            continue

        records.append({
            "Attendance Date": pd.to_datetime(att_date).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
            # "Gate Pass No": gp_no,
            "Employee": emp_code,
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time,
            "Out Time": out_time,
            "Working Hours": work_hrs,
            "Over Time": over_time,
            "Shift": shift,
            "Company": company or "",
            "Branch": branch or "",
        })

    df_final = pd.DataFrame.from_records(
        records,
        columns=[
            "Attendance Date", 
            # "Gate Pass No", 
            "Employee", "Employee Name",
            "Status", "In Time", "Out Time", "Working Hours",
            "Over Time", "Shift", "Company", "Branch",
        ],
    )

    print(f"[clean_daily_inout29] Built final DataFrame with {len(df_final)} rows")

    if df_final.empty:
        raise ValueError("No attendance data parsed from file")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout29] Saved cleaned file: {output_path}")

    if temp_created and os.path.exists(working_file):
        os.unlink(working_file)

    print("[clean_daily_inout29] Done ✅")
    return df_final
