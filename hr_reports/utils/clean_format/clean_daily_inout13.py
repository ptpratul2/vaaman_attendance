# hr_reports/utils/clean_format/clean_daily_inout13.py
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

def _to_float_workhrs(time_str):
    if not time_str or str(time_str).lower() in ["nan", "none"]:
        return 0.0
    try:
        parts = str(time_str).split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return round(h + m/60 + s/3600, 2)
    except Exception:
        return 0.0

def map_status(raw_status) -> str:
    s = "" if pd.isna(raw_status) else str(raw_status).strip()
    mapping = {
        "P": "Present", "POW": "Present",
        "A": "Absent", "AB": "Absent", "O": "Absent",
        "WO": "Holiday", "H": "Holiday",
        "CL": "On Leave", "PL": "On Leave", "SL": "On Leave", "EL": "On Leave", "AP": "On Leave", 
        "RL": "On Leave", "LWP": "On Leave", "SDL": "On Leave", "QL": "On Leave",
        "TU": "On Leave", "CO": "On Leave", "TR": "On Leave", "OH": "On Leave",
        "ML": "On Leave",
        "MIS": "Half Day", "HD": "Half Day", "HALF": "Half Day",
        "E": "Work From Home"
    }
    return mapping.get(s, s if s else "Absent")


def _calculate_overtime(work_hrs_str, shift):
    default_shift_hrs = {"A": 8, "B": 8, "C": 8, "G": 7}
    work_float = _to_float_workhrs(work_hrs_str)
    shift_hrs = default_shift_hrs.get(str(shift).upper(), 0)
    return round(work_float - shift_hrs - 0.60, 2)

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
    elif 10 <= hour < 17:
        return "G"
    return "G"

def clean_daily_inout13(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout13] Starting")
    print(f"[clean_daily_inout13] Input: {input_path}")
    print(f"[clean_daily_inout13] Output: {output_path}")
    print(f"[clean_daily_inout13] Company: {company}")
    print(f"[clean_daily_inout13] Branch: {branch}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df_raw = pd.read_excel(input_path, engine="openpyxl")
    print(f"[clean_daily_inout13] Loaded raw DataFrame shape: {df_raw.shape}")

    required_cols = ["Employee ID", "Attand Date", "Employee Name", "Status", "In Time", "Out Time", "Total Hour"]
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    records = []
    for _, row in df_raw.iterrows():
        emp_id = str(row.get("Employee ID")).strip() if pd.notna(row.get("Employee ID")) else None
        emp_name = str(row.get("Employee Name")).strip() if pd.notna(row.get("Employee Name")) else None
        att_date = row.get("Attand Date")
        time_in = row.get("In Time")
        time_out = row.get("Out Time")
        work_hrs = str(row.get("Total Hour")).strip() if pd.notna(row.get("Total Hour")) else None
        status_raw = row.get("Status")

        # Apply mapping
        status = map_status(status_raw )

         # Skip holidays and blank/empty rows
        if status == "Holiday":
            print(f"[clean_daily_inout13] Skipping {emp_id} {emp_name} on {att_date} (Holiday)")
            continue
        if (pd.isna(time_in) or str(time_in).strip() == "") and \
            (pd.isna(time_out) or str(time_out).strip() == "") and \
            (pd.isna(work_hrs) or str(work_hrs).strip() == "") and \
            (pd.isna(status_raw) or str(status_raw).strip() == ""):
            print(f"[clean_daily_inout13] Skipping {emp_id} {emp_name} on {att_date} (Empty Row)")
            continue


        employee_id = None
        if emp_id:
            try:
                employee_id = frappe.get_value("Employee", {"attendance_device_id": emp_id}, "name")
                if not employee_id:
                    print(f"[clean_daily_inout13] WARNING: No Employee found for device ID {emp_id}")
            except Exception as e:
                print(f"[clean_daily_inout13] ERROR while fetching employee for ID {emp_id}: {e}")


        in_time_fmt = format_datetime(att_date, time_in)
        out_time_fmt = format_datetime(att_date, time_out)
        shift = detect_shift(in_time_fmt, out_time_fmt)
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
    print(f"[clean_daily_inout13] Built final DataFrame with {len(df_final)} rows")

    if df_final.empty:
        raise ValueError("No attendance records parsed from Daily In-Out report.")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout13] Saved output to: {output_path}")
    print("[clean_daily_inout13] Done ✅")

    return df_final
