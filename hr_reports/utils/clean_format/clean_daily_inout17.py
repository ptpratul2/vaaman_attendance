# clean_daily_inout17.py
"""
Cleaner for HIRAKUD PUNCH REPORT FRP (Row-based format).

Key behaviour:
- Row-based format with headers in row 0
- Each row is one attendance record with:
  * Col 0: Contractor
  * Col 1: Workmen (Employee Name)
  * Col 2: IDNo (maps to Employee via attendance_device_id)
  * Col 7: Shift
  * Col 8: Date
  * Col 9: In Time
  * Col 10: Out Time
  * Col 12: Man Hrs
  * Col 13: OT
  * Col 14: Status (P/A/WO/HL/SP)
- Maps IDNo to Employee using attendance_device_id
- Calculates working hours from In Time to Out Time
- Determines status based on working hours (same as clean_daily_inout16):
  * >= 7.0 hours → Present
  * >= 4.5 hours → Half Day
  * < 4.5 hours → Absent
  * Special codes (WO/PH/PL/LI) → handled separately
- Detects shift based on In Time (A/G/B/C)
- Calculates overtime (work hours - 9)
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import frappe
import pandas as pd


# -------------------------
# Helpers
# -------------------------
def parse_time_with_date(date_val, time_val) -> Optional[str]:
    """
    Parse date and time values and combine them.
    Returns 'YYYY-MM-DD HH:MM:SS'
    """
    if pd.isna(date_val) or pd.isna(time_val):
        return None

    try:
        # Parse date
        if isinstance(date_val, datetime):
            date_obj = date_val
        else:
            date_obj = pd.to_datetime(date_val)

        # Parse time
        if isinstance(time_val, datetime):
            time_obj = time_val
        else:
            time_obj = pd.to_datetime(time_val, format='%H:%M:%S').time()

        # Combine date and time
        combined = datetime.combine(date_obj.date(), time_obj.time() if hasattr(time_obj, 'time') else time_obj)
        return combined.strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        print(f"[parse_time_with_date] Error parsing date '{date_val}' time '{time_val}': {e}")
        return None


def calculate_working_hours(in_time: Optional[str], out_time: Optional[str]) -> float:
    """
    Calculate working hours between in_time and out_time.
    Handles overnight shifts.
    Returns hours in decimal format.
    """
    if not in_time or not out_time:
        return 0.0

    try:
        in_dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        out_dt = datetime.strptime(out_time, "%Y-%m-%d %H:%M:%S")

        # Handle overnight shifts
        if out_dt < in_dt:
            out_dt = out_dt + timedelta(days=1)

        diff = out_dt - in_dt
        hours = diff.total_seconds() / 3600

        return round(hours, 2)
    except Exception as e:
        print(f"[calculate_working_hours] Error: {e}")
        return 0.0


def format_working_hours(hours: float) -> str:
    """Convert decimal hours to HH:MM format"""
    if hours <= 0:
        return "00:00"

    h = int(hours)
    m = int((hours - h) * 60)
    return f"{h:02d}:{m:02d}"


def determine_status_from_hours(working_hours: float, status_code: str) -> str:
    """
    Determine attendance status based on working hours and status code.
    Same logic as clean_daily_inout16:
    - WO/PH/PL/LI status codes → treated as is
    - >= 7.0 hours → "Present"
    - >= 4.5 hours → "Half Day"
    - < 4.5 hours → "Absent"
    """
    # Handle special status codes first
    code = str(status_code).strip().upper() if pd.notna(status_code) else ""

    status_map = {
        "WO": "Present",    # Weekly Off
        "PH": "On Leave",   # Public Holiday
        "HL": "On Leave",   # Holiday Leave
        "PL": "On Leave",   # Paid Leave
        "LI": "On Leave",   # Leave
        "UL": "Absent",     # Unpaid Leave
        "A": "Absent",      # Absent
        "SP": "Absent",     # Special (treated as absent if no hours)
    }

    if code in status_map:
        # For WO and leave codes, return mapped status
        if code in ["WO", "PH", "HL", "PL", "LI"]:
            return status_map[code]
        # For A/UL, check if there are working hours
        elif working_hours > 0:
            # Has hours, so calculate based on hours
            pass
        else:
            return status_map[code]

    # For P (Present) and others, or codes with working hours, determine by working hours
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def detect_shift_from_time(in_time: Optional[str]) -> str:
    """
    Detect shift based on In Time (same as clean_daily_inout16).
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
            # Find nearest shift
            distances = {
                "A": abs(hour - 6),
                "G": abs(hour - 9),
                "B": abs(hour - 14),
                "C": abs(hour - 22) if hour > 12 else abs(hour + 24 - 22)
            }
            return min(distances, key=distances.get)
    except Exception:
        return ""


def normalize_id(id_val: str) -> str:
    """
    Normalize ID by removing leading/trailing spaces.
    Examples:
    - VEIL 046 → VEIL 046
    - VEIL046 → VEIL046
    """
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

    shift_hours = 9
    overtime = round(work_hours - shift_hours, 2)

    if overtime < 1:
        return ""

    return str(overtime)


# -------------------------
# Main cleaning function
# -------------------------
def clean_daily_inout17(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout17] Starting - HIRAKUD PUNCH REPORT FRP")
    print(f"[clean_daily_inout17] Input: {input_path}")
    print(f"[clean_daily_inout17] Output: {output_path}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read the Excel file with headers
    df = pd.read_excel(input_path, header=0)
    print(f"[clean_daily_inout17] Raw shape: {df.shape}")
    print(f"[clean_daily_inout17] Columns: {df.columns.tolist()}")

    # Process each row
    records = []

    for idx, row in df.iterrows():
        try:
            # Extract data from columns
            contractor = str(row.get('Contractor', '')).strip() if pd.notna(row.get('Contractor')) else ""
            workmen = str(row.get('Workmen', '')).strip() if pd.notna(row.get('Workmen')) else ""
            id_no = str(row.get('IDNo', '')).strip() if pd.notna(row.get('IDNo')) else ""
            date_val = row.get('Date')
            in_time_val = row.get('In Time')
            out_time_val = row.get('Out Time')
            status_code = str(row.get('Status', '')).strip() if pd.notna(row.get('Status')) else ""

            if not id_no or pd.isna(date_val):
                continue

            # Normalize ID
            id_normalized = normalize_id(id_no)

            # Resolve Employee from IDNo using attendance_device_id
            try:
                emp_code = frappe.db.get_value("Employee", {"attendance_device_id": id_normalized}, "name")
                if not emp_code:
                    emp_code = ""  # Blank, not skip
                    print(f"[clean_daily_inout17] Warning: No Employee found for IDNo {id_normalized} - keeping blank")
            except Exception as e:
                emp_code = ""  # Blank on error
                print(f"[clean_daily_inout17] Error looking up IDNo {id_normalized}: {e} - keeping blank")

            # Parse date
            if isinstance(date_val, datetime):
                date_str = date_val.strftime("%Y-%m-%d")
            else:
                date_obj = pd.to_datetime(date_val)
                date_str = date_obj.strftime("%Y-%m-%d")

            # Parse IN and OUT times
            in_time = parse_time_with_date(date_val, in_time_val)
            out_time = parse_time_with_date(date_val, out_time_val)

            # Calculate working hours from IN and OUT times
            work_hours_decimal = calculate_working_hours(in_time, out_time)
            work_hours_formatted = format_working_hours(work_hours_decimal)

            # Determine status based on working hours
            status = determine_status_from_hours(work_hours_decimal, status_code)

            # Detect shift from IN time
            shift_code = detect_shift_from_time(in_time)

            # Calculate overtime
            overtime = calculate_overtime(work_hours_decimal)

            # Build record
            record = {
                "Attendance Date": date_str,
                "Employee": emp_code,
                "Employee Name": workmen,
                "Status": status,
                "In Time": in_time or "",
                "Out Time": out_time or "",
                "Working Hours": work_hours_formatted,
                "Over Time": overtime,
                "Shift": shift_code,
                "Company": company if company else contractor,
                "Branch": branch if branch else "",
            }

            records.append(record)

        except Exception as e:
            print(f"[clean_daily_inout17] Error processing row {idx}: {e}")
            continue

    # Create final dataframe
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

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout17] Saved cleaned file: {output_path}")
    print("[clean_daily_inout17] Done ✅")

    return df_final
