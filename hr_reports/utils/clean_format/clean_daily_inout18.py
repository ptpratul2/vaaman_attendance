# clean_daily_inout18.py
"""
Cleaner for HIRAKUD SMELTER PUNCH TIMING (Row-based format).

Key behaviour:
- Row-based format with headers in row 0
- Each row is one attendance record with:
  * Col 1: Contractor Token No (IDNo - maps to Employee via attendance_device_id)
  * Col 2: Labour Name (Employee Name)
  * Col 5: Shift (B-SHIFT, C-SHIFT, G-SHIFT, A-SHIFT, etc.)
  * Col 6: Status (P/A/P/WO/P/HL/SP/HD)
  * Col 8: Check In Date
  * Col 9: Check In Time
  * Col 10: Check Out Date
  * Col 11: Check Out Time
  * Col 12: Man Hours (decimal)
  * Col 14: OT (decimal)
- Maps Contractor Token No to Employee using attendance_device_id
- Calculates working hours from Check In Time to Check Out Time
- Determines status based on working hours (same as clean_daily_inout17):
  * >= 7.0 hours → Present
  * >= 4.5 hours → Half Day
  * < 4.5 hours → Absent
  * Special codes (WO/PH/PL/LI) → handled separately
- Uses Shift column value or detects shift based on In Time (A/G/B/C)
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
        elif isinstance(time_val, str):
            # Parse string time like "06:24:00"
            time_obj = pd.to_datetime(time_val, format='%H:%M:%S').time()
        else:
            time_obj = time_val

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
    Same logic as clean_daily_inout17:
    - WO/PH/PL/LI/HL status codes → treated as is
    - >= 7.0 hours → "Present"
    - >= 4.5 hours → "Half Day"
    - < 4.5 hours → "Absent"
    """
    # Handle special status codes first
    code = str(status_code).strip().upper() if pd.notna(status_code) else ""

    # Handle composite codes like "P/WO", "P/HL"
    if "/" in code:
        parts = code.split("/")
        # Take the second part (WO, HL, etc.)
        code = parts[1] if len(parts) > 1 else parts[0]

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

    # Handle HD (Half Day) code
    if code == "HD":
        return "Half Day"

    # For P (Present) and others, or codes with working hours, determine by working hours
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def detect_shift_from_time(in_time: Optional[str]) -> str:
    """
    Detect shift based on In Time (same as clean_daily_inout17).
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


def normalize_shift_code(shift_val: str) -> str:
    """
    Normalize shift code from Excel format to single letter.
    Examples:
    - B-SHIFT → B
    - C-SHIFT → C
    - G-SHIFT → G
    - A-SHIFT → A
    - SECURITY SHIFT-1/2/3 → blank (will auto-detect from time)
    - Out Of Shift → blank (will auto-detect from time)
    """
    if not shift_val or pd.isna(shift_val):
        return ""

    shift_str = str(shift_val).strip().upper()

    if shift_str == "A-SHIFT":
        return "A"
    elif shift_str == "G-SHIFT":
        return "G"
    elif shift_str == "B-SHIFT":
        return "B"
    elif shift_str == "C-SHIFT":
        return "C"
    elif "SECURITY SHIFT" in shift_str:
        return ""  # Return blank, will be auto-detected from check-in time
    elif "OUT OF SHIFT" in shift_str:
        return ""  # Return blank, will be auto-detected from check-in time
    else:
        return ""


def normalize_id(id_val: str) -> str:
    """
    Normalize ID by removing leading/trailing spaces.
    Examples:
    - SML008376 → SML008376
    - SML 008376 → SML 008376
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
def clean_daily_inout18(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout18] Starting - HIRAKUD SMELTER PUNCH TIMING")
    print(f"[clean_daily_inout18] Input: {input_path}")
    print(f"[clean_daily_inout18] Output: {output_path}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read the Excel file with headers
    df = pd.read_excel(input_path, header=0)
    print(f"[clean_daily_inout18] Raw shape: {df.shape}")
    print(f"[clean_daily_inout18] Columns: {df.columns.tolist()}")

    # Process each row
    records = []

    for idx, row in df.iterrows():
        try:
            # Extract data from columns (note: column names have spaces)
            token_no = str(row.get(' Contractor Token No ', '')).strip() if pd.notna(row.get(' Contractor Token No ')) else ""
            labour_name = str(row.get(' Labour Name ', '')).strip() if pd.notna(row.get(' Labour Name ')) else ""
            shift_val = row.get('Shift', '')
            status_code = str(row.get('Status', '')).strip() if pd.notna(row.get('Status')) else ""
            check_in_date = row.get('Check In Date')
            check_in_time = row.get('Check In Time')
            check_out_date = row.get('Check Out Date')
            check_out_time = row.get('Check Out Time')

            if not token_no or pd.isna(check_in_date):
                continue

            # Normalize ID
            id_normalized = normalize_id(token_no)

            # Resolve Employee from Token No using attendance_device_id
            try:
                emp_code = frappe.db.get_value("Employee", {"attendance_device_id": id_normalized}, "name")
                if not emp_code:
                    emp_code = ""  # Blank, not skip
                    print(f"[clean_daily_inout18] Warning: No Employee found for Token No {id_normalized} - keeping blank")
            except Exception as e:
                emp_code = ""  # Blank on error
                print(f"[clean_daily_inout18] Error looking up Token No {id_normalized}: {e} - keeping blank")

            # Parse date
            if isinstance(check_in_date, datetime):
                date_str = check_in_date.strftime("%Y-%m-%d")
            else:
                date_obj = pd.to_datetime(check_in_date)
                date_str = date_obj.strftime("%Y-%m-%d")

            # Parse IN and OUT times
            in_time = parse_time_with_date(check_in_date, check_in_time)
            out_time = parse_time_with_date(check_out_date, check_out_time)

            # Calculate working hours from IN and OUT times
            work_hours_decimal = calculate_working_hours(in_time, out_time)
            work_hours_formatted = format_working_hours(work_hours_decimal)

            # Determine status based on working hours
            status = determine_status_from_hours(work_hours_decimal, status_code)

            # Use shift from Excel or detect from IN time
            shift_code = normalize_shift_code(shift_val)
            if not shift_code:
                shift_code = detect_shift_from_time(in_time)

            # Calculate overtime
            overtime = calculate_overtime(work_hours_decimal)

            # Build record
            record = {
                "Attendance Date": date_str,
                "Employee": emp_code,
                "Employee Name": labour_name,
                "Status": status,
                "In Time": in_time or "",
                "Out Time": out_time or "",
                "Working Hours": work_hours_formatted,
                "Over Time": overtime,
                "Shift": shift_code,
                "Company": company if company else "",
                "Branch": branch if branch else "",
            }

            records.append(record)

        except Exception as e:
            print(f"[clean_daily_inout18] Error processing row {idx}: {e}")
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

    print(f"[clean_daily_inout18] Total records parsed: {len(df_final)}")

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout18] Saved cleaned file: {output_path}")
    print("[clean_daily_inout18] Done ✅")

    return df_final
