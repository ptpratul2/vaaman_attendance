# clean_daily_inout17.py
"""
Cleaner for HIRAKUD PUNCH REPORT (FRP and Smelter share the same layout).

Source file layout (row-based, headers in row 0):
  - Sr.No.
  - Contractor Name
  - Contractor Token No   (attendance_device_id, e.g. "VEIL 045" / "SML008376")
  - Labour Name
  - Date
  - Check In Date / Check In Time
  - Check Out Date / Check Out Time   (separate date column, so overnight
    shifts don't need manual +1 day handling)
  - Status                 (P / HD / A / SP / P/HL / P/WO)
  - Man Hours, OT           (source's own payroll figures - not used; we
    recompute both from the Check In/Out timestamps instead, since the
    source values are formatted as H.MM, not decimal hours, and encode
    contractor-specific holiday/OT payroll rules)

Status mapping (source Status -> Attendance Status):
  - P      -> Present
  - HD     -> Half Day
  - A      -> Absent
  - SP     -> Absent   (single punch, no checkout recorded)
  - P/HL   -> Half Day (present on a holiday)
  - P/WO   -> Present  (present on a weekly-off)

Working Hours / Overtime are computed from Check In -> Check Out and stored
as decimal floats (e.g. 8.42), NOT "HH:MM" strings - the Attendance
doctype's working_hours field is a Float, and Frappe's flt("08:25") silently
returns 0.0, which is why "HH:MM" strings never actually land on the saved
Attendance record even though Data Import's preview shows them fine.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import frappe
import pandas as pd


# -------------------------
# Helpers
# -------------------------
def combine_date_time(date_val, time_val) -> Optional[str]:
    """
    Combine a date value and a time value into 'YYYY-MM-DD HH:MM:SS'.
    """
    if pd.isna(date_val) or pd.isna(time_val):
        return None

    try:
        date_obj = date_val if isinstance(date_val, datetime) else pd.to_datetime(date_val)

        if isinstance(time_val, datetime):
            time_obj = time_val.time()
        else:
            time_obj = pd.to_datetime(str(time_val), format="%H:%M:%S").time()

        combined = datetime.combine(date_obj.date(), time_obj)
        return combined.strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        print(f"[combine_date_time] Error parsing date '{date_val}' time '{time_val}': {e}")
        return None


def calculate_working_hours(in_time: Optional[str], out_time: Optional[str]) -> float:
    """
    Calculate working hours between in_time and out_time.
    Check In/Out already carry their own dates, so this only needs to guard
    against the rare case where the source dates were entered same-day for
    an overnight punch.
    Returns hours in decimal format.
    """
    if not in_time or not out_time:
        return 0.0

    try:
        in_dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        out_dt = datetime.strptime(out_time, "%Y-%m-%d %H:%M:%S")

        if out_dt < in_dt:
            out_dt = out_dt + timedelta(days=1)

        diff = out_dt - in_dt
        return round(diff.total_seconds() / 3600, 2)
    except Exception as e:
        print(f"[calculate_working_hours] Error: {e}")
        return 0.0


def detect_shift_from_time(in_time: Optional[str]) -> str:
    """
    Detect shift based on In Time.
    - A shift: 05:00 - 07:00
    - G shift: 08:00 - 10:00
    - B shift: 13:00 - 15:00
    - C shift: 21:00 - 23:00
    - Returns blank if no IN time
    """
    if not in_time or str(in_time).strip() == "":
        return ""

    try:
        in_dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        hour = in_dt.hour

        if 5 <= hour <= 7:
            return "A"
        elif 8 <= hour <= 10:
            return "G"
        elif 13 <= hour <= 15:
            return "B"
        elif 21 <= hour <= 23:
            return "C"
        else:
            distances = {
                "A": abs(hour - 6),
                "G": abs(hour - 9),
                "B": abs(hour - 14),
                "C": abs(hour - 22) if hour > 12 else abs(hour + 24 - 22),
            }
            return min(distances, key=distances.get)
    except Exception:
        return ""


def normalize_id(id_val: str) -> str:
    """Normalize ID by removing leading/trailing spaces."""
    if not id_val:
        return ""
    return str(id_val).strip()


def calculate_overtime(work_hours: float) -> str:
    """
    Calculate overtime.
    Formula: OT = Working Hours - 9
    Returns blank if OT < 1 hour
    """
    if not work_hours or work_hours <= 0:
        return ""

    overtime = round(work_hours - 9, 2)
    if overtime < 1:
        return ""

    return str(overtime)


STATUS_MAP = {
    "P": "Present",
    "HD": "Half Day",
    "A": "Absent",
    "SP": "Absent",
    "P/HL": "Half Day",
    "P/WO": "Present",
}


def map_status(status_code: str, working_hours: float) -> str:
    """
    Map the source Status code to an Attendance Status.
    Falls back to an hours-based guess for any unrecognized code so unknown
    codes don't silently get dropped or crash the run.
    """
    code = str(status_code).strip().upper() if pd.notna(status_code) else ""

    if code in STATUS_MAP:
        return STATUS_MAP[code]

    print(f"[map_status] Unrecognized status code '{status_code}' - falling back to hours-based status")
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


# -------------------------
# Main cleaning function
# -------------------------
def clean_daily_inout17(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout17] Starting - HIRAKUD PUNCH REPORT (FRP / Smelter)")
    print(f"[clean_daily_inout17] Input: {input_path}")
    print(f"[clean_daily_inout17] Output: {output_path}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_excel(input_path, header=0)
    df.columns = df.columns.str.strip()
    print(f"[clean_daily_inout17] Raw shape: {df.shape}")
    print(f"[clean_daily_inout17] Columns: {df.columns.tolist()}")

    required_cols = [
        "Contractor Name",
        "Contractor Token No",
        "Labour Name",
        "Date",
        "Check In Date",
        "Check In Time",
        "Check Out Date",
        "Check Out Time",
        "Status",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    records = []

    for idx, row in df.iterrows():
        try:
            contractor = str(row.get("Contractor Name", "")).strip() if pd.notna(row.get("Contractor Name")) else ""
            workmen = str(row.get("Labour Name", "")).strip() if pd.notna(row.get("Labour Name")) else ""
            token = str(row.get("Contractor Token No", "")).strip() if pd.notna(row.get("Contractor Token No")) else ""
            date_val = row.get("Date")
            status_code = row.get("Status")

            if not token or pd.isna(date_val):
                continue

            id_normalized = normalize_id(token)

            try:
                emp_code = frappe.db.get_value("Employee", {"attendance_device_id": id_normalized}, "name")
                if not emp_code:
                    emp_code = ""
                    print(f"[clean_daily_inout17] Warning: No Employee found for Token {id_normalized} - keeping blank")
            except Exception as e:
                emp_code = ""
                print(f"[clean_daily_inout17] Error looking up Token {id_normalized}: {e} - keeping blank")

            date_obj = date_val if isinstance(date_val, datetime) else pd.to_datetime(date_val)
            date_str = date_obj.strftime("%Y-%m-%d")

            in_time = combine_date_time(row.get("Check In Date"), row.get("Check In Time"))
            out_time = combine_date_time(row.get("Check Out Date"), row.get("Check Out Time"))

            work_hours_decimal = calculate_working_hours(in_time, out_time)
            status = map_status(status_code, work_hours_decimal)
            shift_code = detect_shift_from_time(in_time)
            overtime = calculate_overtime(work_hours_decimal)

            record = {
                "Attendance Date": date_str,
                "Employee": emp_code,
                "Employee Name": workmen,
                "Status": status,
                "In Time": in_time or "",
                "Out Time": out_time or "",
                "Working Hours": work_hours_decimal,
                "Over Time": overtime,
                "Shift": shift_code,
                "Company": company if company else contractor,
                "Branch": branch if branch else "",
            }

            records.append(record)

        except Exception as e:
            print(f"[clean_daily_inout17] Error processing row {idx}: {e}")
            continue

    df_final = pd.DataFrame.from_records(
        records,
        columns=[
            "Attendance Date",
            "Employee",
            "Employee Name",
            "Status",
            "In Time",
            "Out Time",
            "Working Hours",
            "Over Time",
            "Shift",
            "Company",
            "Branch",
        ],
    )

    if df_final.empty:
        raise ValueError(
            "❌ No attendance records could be parsed. "
            "Please check that the file format is correct."
        )

    print(f"[clean_daily_inout17] Total records parsed: {len(df_final)}")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout17] Saved cleaned file: {output_path}")
    print("[clean_daily_inout17] Done ✅")

    return df_final
