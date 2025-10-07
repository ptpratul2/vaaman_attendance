# hr_reports/utils/clean_format/clean_daily_inout10.py
import os
import pandas as pd
import frappe
from datetime import datetime, timedelta
from typing import Optional

def format_datetime(date_val, time_val):
    if pd.isna(date_val):
        return None

    if not isinstance(date_val, (datetime, pd.Timestamp)):
        date_val = pd.to_datetime(date_val, errors="coerce")
    if pd.isna(date_val):
        return None

    if isinstance(time_val, timedelta):
        total_seconds = int(time_val.total_seconds())
        hours = (total_seconds // 3600) % 24
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
    else:
        try:
            t_str = str(time_val).strip()
            if not t_str or t_str.lower() in ["nan", "none"]:
                return None
            parts = t_str.replace(".", ":").split(":")
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
            seconds = int(parts[2]) if len(parts) > 2 else 0
        except Exception:
            return None

    suffix = "AM"
    if hours >= 12:
        suffix = "PM"
        if hours > 12:
            hours -= 12
    elif hours == 0:
        hours = 12

    return date_val.strftime("%Y-%m-%d") + f" {hours:02d}:{minutes:02d}:{seconds:02d}"

def _to_float_workhrs(time_val):
    if not time_val or str(time_val).lower() in ["nan", "none"]:
        return 0.0

    # Handle datetime directly
    if isinstance(time_val, (datetime, pd.Timestamp)):
        h = time_val.hour
        m = time_val.minute
        return float(f"{h:02d}.{m:02d}")

    # Handle string like "08:15" or "1900-01-24 08:15:13"
    try:
        dt = pd.to_datetime(time_val, errors="coerce")
        if pd.isna(dt):
            return 0.0
        h = dt.hour
        m = dt.minute
        return float(f"{h:02d}.{m:02d}")
    except Exception:
        return 0.0




def map_status(raw_status) -> str:
    s = "" if pd.isna(raw_status) else str(raw_status).strip()
    mapping = {
        "P": "Present",
        "A": "Absent",
        "HD": "Half Day",
        "P/2": "Half Day",
        "H": "Holiday",
        "WO": "Holiday",
        "0": "On Leave"
    }
    return mapping.get(s, s if s else "Absent")

def detect_shift(in_time: Optional[str], out_time: Optional[str]) -> str:
    def get_hour(ts: Optional[str]) -> Optional[int]:
        if not ts or str(ts).strip() == "" or pd.isna(ts):
            return None
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").hour
        except Exception:
            return None

    in_hour = get_hour(in_time)
    out_hour = get_hour(out_time)

    hour = in_hour if in_hour is not None else out_hour
    if hour is None:
        return "G"

    if 6 <= hour < 14:
        return "A"
    elif 14 <= hour < 22:
        return "B"
    elif hour >= 22 or hour < 6:
        return "C"
    return "G"

def _calculate_overtime(work_hrs_str, shift):
    default_shift_hrs = {"A": 8, "B": 8, "C": 8, "G": 7}
    work_float = _to_float_workhrs(work_hrs_str)
    shift_hrs = default_shift_hrs.get(str(shift).upper(), 0)
    return round(work_float - shift_hrs - 0.60, 2)

def clean_daily_inout10(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("="*80)
    print("[clean_daily_inout10] Starting")
    print(f"Input: {input_path}, Output: {output_path}")
    print(f"Company: {company}, Branch: {branch}")
    print("="*80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df_raw = pd.read_excel(input_path, engine="openpyxl")
    print(f"[clean_daily_inout10] Loaded raw DataFrame shape: {df_raw.shape}")

    required_cols = ["Date", "Employee ID", "Employee Name", "PRESENT", "IN Time Punch", "OUT Time Punch", "AWH", "OT", "SHIFT"]
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    records = []
    for _, row in df_raw.iterrows():
        emp_id = row.get("Employee ID") if pd.notna(row.get("Employee ID")) else None
        emp_name = str(row.get("Employee Name")).strip() if pd.notna(row.get("Employee Name")) else None
        att_date = row.get("Date")
        time_in = row.get("IN Time Punch")
        time_out = row.get("OUT Time Punch")
        work_hrs_val = row.get("AWH")  # keep as datetime or float
        work_hrs = _to_float_workhrs(work_hrs_val)
        ot_hrs = str(row.get("OT")).strip() if pd.notna(row.get("OT")) else None
        shift_raw = str(row.get("SHIFT")).strip() if pd.notna(row.get("SHIFT")) else "G"
        status_raw = row.get("PRESENT")

        # Map status
        status = map_status(status_raw)

        # Skip empty rows
        if (pd.isna(time_in) and pd.isna(time_out) and pd.isna(work_hrs) and pd.isna(status_raw)):
            continue

        employee_id = emp_id

        in_time_fmt = format_datetime(att_date, time_in)
        out_time_fmt = format_datetime(att_date, time_out)
        shift = shift_raw if shift_raw else detect_shift(in_time_fmt, out_time_fmt)
        overtime_val = _calculate_overtime(work_hrs, shift)

        rec = {
            "Attendance Date": pd.to_datetime(att_date).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
            "Employee": employee_id if employee_id else "",
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time_fmt,
            "Out Time": out_time_fmt,
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": work_hrs,
            "Shift": shift,
            "Over Time": overtime_val
        }
        records.append(rec)

    df_final = pd.DataFrame.from_records(records)
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")
    print(f"[clean_daily_inout10] Built final DataFrame with {len(df_final)} rows")

    if df_final.empty:
        raise ValueError("No attendance records parsed from Daily In-Out report.")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout10] Saved output to: {output_path}")
    print("[clean_daily_inout10] Done ✅")

    return df_final
