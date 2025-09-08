import os
import pandas as pd
import frappe
from datetime import datetime, timedelta

def format_datetime(date_val, time_val):
    """
    Combine Date + Timedelta/Time into dd-mm-YYYY hh:mm:ss AM/PM format.
    Fallback: use 09:00 AM for In, 05:00 PM for Out if missing.
    """
    if pd.isna(date_val):
        return None

    # Normalize date
    if not isinstance(date_val, (datetime, pd.Timestamp)):
        date_val = pd.to_datetime(date_val, errors="coerce")
    if pd.isna(date_val):
        return None

    # If time is timedelta (from Excel)
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

    # AM/PM adjustment
    suffix = "AM"
    if hours >= 12:
        suffix = "PM"
        if hours > 12:
            hours -= 12
    elif hours == 0:
        hours = 12  # midnight = 12 AM

    return date_val.strftime("%d-%m-%Y") + f" {hours:02d}:{minutes:02d}:{seconds:02d} {suffix}"


def _to_float_workhrs(time_str):
    """Convert 'HH:MM:SS' → float hours e.g. '08:53:09' → 8.53"""
    if not time_str or str(time_str).lower() in ["nan", "none"]:
        return 0.0
    try:
        parts = str(time_str).split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return round(h + m/60 + s/3600, 2)  # keep 2 decimals
    except Exception:
        return 0.0


def _calculate_overtime(work_hrs_str, shift):
    """Overtime calculation as per logic"""
    default_shift_hrs = {"A": 8, "B": 8, "C": 8, "G": 7}
    work_float = _to_float_workhrs(work_hrs_str)
    shift_hrs = default_shift_hrs.get(str(shift).upper(), 0)
    overtime = round(work_float - shift_hrs - 0.60, 2)
    return overtime


def clean_daily_inout14(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout14] Starting")
    print(f"[clean_daily_inout14] Input: {input_path}")
    print(f"[clean_daily_inout14] Output: {output_path}")
    print(f"[clean_daily_inout14] Company: {company}")
    print(f"[clean_daily_inout14] Branch: {branch}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Load file
    df_raw = pd.read_excel(input_path, engine="openpyxl")
    print(f"[clean_daily_inout14] Loaded raw DataFrame shape: {df_raw.shape}")

    # Required cols
    required_cols = ["GP No", "Name", "Date In", "Time In", "Time Out", "Working Hours", "Came In Shift"]
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    records = []
    for _, row in df_raw.iterrows():
        gp_no = str(row.get("GP No")).strip() if pd.notna(row.get("GP No")) else None
        emp_name = str(row.get("Name")).strip() if pd.notna(row.get("Name")) else None
        att_date = row.get("Date In")
        time_in = row.get("Time In")
        time_out = row.get("Time Out")
        work_hrs = str(row.get("Working Hours")).strip() if pd.notna(row.get("Working Hours")) else None
        shift = str(row.get("Came In Shift")).strip() if pd.notna(row.get("Came In Shift")) else None

        # Map GP No → Employee
        employee_id = None
        if gp_no:
            try:
                emp_doc = frappe.get_doc("Employee", {"attendance_device_id": gp_no})
                employee_id = emp_doc.name
            except Exception:
                print(f"[clean_daily_inout14] WARNING: Employee not found for GP No {gp_no}")

        # Status logic
        status = "Absent"
        if work_hrs and work_hrs not in ["", "0:00", "00:00"]:
            status = "Present"

        # Format In/Out time
        in_time_fmt = format_datetime(att_date, time_in) or format_datetime(att_date, "09:00:00")
        out_time_fmt = format_datetime(att_date, time_out) or format_datetime(att_date, "17:00:00")

        # Overtime calculation
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
            "Shift": shift if shift else "",
            "Over Time": overtime_val
        }
        records.append(rec)

    df_final = pd.DataFrame.from_records(records)

    # Drop invalid
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")
    print(f"[clean_daily_inout14] Built final DataFrame with {len(df_final)} rows")

    if df_final.empty:
        raise ValueError("No attendance records parsed from Daily In-Out report.")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout14] Saved output to: {output_path}")
    print("[clean_daily_inout14] Done ✅")

    return df_final
