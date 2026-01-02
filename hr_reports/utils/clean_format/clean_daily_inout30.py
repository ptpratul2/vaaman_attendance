# clean_daily_inout30.py
"""
Cleaner for Transaction IN OUT Report (vertical format).

Key behaviour:
- Simple vertical format where each row is already an attendance record
- Columns: Company, Date, Contractor Name, EMP ID, Ramco EMP ID,
  Contractor Workers Name, Vendor Code, Work Order No, IN PUNCH, OUT PUNCH, Pass Type
- Maps 'Ramco EMP ID' to Employee (GP NO)
- **Handles duplicate punches**: Groups by employee + date, uses FIRST IN and LAST OUT
- Calculates Working Hours from first IN to last OUT (handles overnight shifts)
- Detects shift based on punch-in time windows:
  * A shift: punch between 5-7 (05:00 to 07:00)
  * G shift: punch between 8-10 (08:00 to 10:00)
  * B shift: punch between 13-15 (13:00 to 15:00)
  * C shift: punch between 21-23 (21:00 to 23:00)
- Determines status based on working hours:
  * >= 7.0 hours → Present
  * >= 4.5 hours → Half Day
  * < 4.5 hours → Absent
- Calculates overtime: OT = Hours - 9 (only shown if >= 1 hour)
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import frappe
import pandas as pd


# -------------------------
# Helpers
# -------------------------
def parse_date(date_str) -> Optional[str]:
    """Parse date from various formats to YYYY-MM-DD"""
    if pd.isna(date_str) or str(date_str).strip() == "":
        return None

    try:
        # Try common date formats
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y"]:
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except:
                continue

        # Try pandas to_datetime as fallback
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[parse_date] Error parsing date '{date_str}': {e}")

    return None


def parse_time(date_str: str, time_val, is_checkin: bool) -> Optional[str]:
    """
    Parse time value and return 'YYYY-MM-DD HH:MM:SS'
    Handles HH:MM:SS, HH:MM formats
    """
    if time_val is None or pd.isna(time_val) or str(time_val).strip() == "":
        return None

    try:
        time_str = str(time_val).strip()

        # Handle HH:MM:SS format
        if ":" in time_str:
            parts = time_str.split(":")
            if len(parts) >= 2:
                h = int(parts[0])
                m = int(parts[1])
                s = int(parts[2]) if len(parts) > 2 else 0
                return f"{date_str} {h:02d}:{m:02d}:{s:02d}"

        # If it's a datetime object
        if isinstance(time_val, (datetime, pd.Timestamp)):
            return f"{date_str} {time_val.hour:02d}:{time_val.minute:02d}:{time_val.second:02d}"

    except Exception as e:
        print(f"[parse_time] Error parsing time '{time_val}': {e}")

    # Return None if parsing fails
    return None


def calculate_working_hours(in_time: Optional[str], out_time: Optional[str]) -> float:
    """
    Calculate working hours between in_time and out_time.

    Logic:
    - Calculates: Out Time - In Time
    - Handles overnight shifts: If out_time < in_time, adds 1 day to out_time
    - Returns hours in decimal format (e.g., 8.50 for 8 hours 30 minutes)
    - Rounded to 2 decimal places

    Example:
    - In: 08:30, Out: 17:00 → 8.5 hours
    - In: 22:00, Out: 06:00 (next day) → 8 hours (overnight shift)
    """
    if not in_time or not out_time:
        return 0.0

    try:
        # Parse timestamps
        in_dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        out_dt = datetime.strptime(out_time, "%Y-%m-%d %H:%M:%S")

        # Handle overnight shifts: if out_time < in_time, add 1 day to out_time
        if out_dt < in_dt:
            out_dt = out_dt + timedelta(days=1)

        # Calculate difference
        diff = out_dt - in_dt

        # Convert to hours
        hours = diff.total_seconds() / 3600

        # Round to 2 decimal places
        return round(hours, 2)

    except Exception as e:
        print(f"[calculate_working_hours] Error calculating hours for {in_time} to {out_time}: {e}")
        return 0.0


def format_working_hours(hours: float) -> str:
    """Convert decimal hours to HH:MM format"""
    if hours <= 0:
        return "00:00"

    h = int(hours)
    m = int((hours - h) * 60)
    return f"{h:02d}:{m:02d}"


def detect_shift(in_time: Optional[str], out_time: Optional[str]) -> str:
    """
    Detect shift code based on punch-in time windows.

    Shift definitions (punch-in time windows):
    - A shift: punch between 5-7 (05:00 to 07:00)
    - G shift: punch between 8-10 (08:00 to 10:00)
    - B shift: punch between 13-15 (13:00 to 15:00)
    - C shift: punch between 21-23 (21:00 to 23:00)

    If punch time is outside these windows, returns nearest shift.
    """
    if not in_time or str(in_time).strip() == "":
        return "G"

    try:
        # Parse in_time to get hour
        in_dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        hour = in_dt.hour

        # Check which shift window the punch-in falls into
        if 5 <= hour <= 7:
            return "A"
        elif 8 <= hour <= 10:
            return "G"
        elif 13 <= hour <= 15:
            return "B"
        elif 21 <= hour <= 23:
            return "C"
        else:
            # If outside all windows, find nearest shift
            # A center: 6, G center: 9, B center: 14, C center: 22
            distances = {
                "A": abs(hour - 6),
                "G": abs(hour - 9),
                "B": abs(hour - 14),
                "C": abs(hour - 22) if hour > 12 else abs(hour + 24 - 22)
            }
            return min(distances, key=distances.get)

    except Exception:
        return "G"


def calculate_overtime(work_hours: float) -> str:
    """
    Calculate overtime hours.

    Logic:
    - Standard shift hours: 9 hours (hardcoded)
    - Formula: OT = Working Hours - 9
    - Rules:
      - If OT < 1 hour → return blank (empty string)
      - If OT >= 1 hour → return OT value rounded to 2 decimals
      - If working_hours is 0 or None → return blank

    Example:
    - Working Hours = 12.5 → OT = 12.5 - 9 = 3.5 hours
    - Working Hours = 9.5 → OT = 9.5 - 9 = 0.5 → blank (less than 1 hour)
    """
    if not work_hours or work_hours <= 0:
        return ""

    shift_hours = 9
    overtime = round(work_hours - shift_hours, 2)

    # Return blank if less than 1 hour
    if overtime < 1:
        return ""

    return str(overtime)


def determine_status(working_hours: float) -> str:
    """
    Determine attendance status based on working hours thresholds.

    Logic based on Working Hours thresholds:
    - >= 7.0 hours → "Present"
    - >= 4.5 hours (but < 7.0) → "Half Day"
    - < 4.5 hours → "Absent"

    Example:
    - 8.5 hours → Present
    - 5.0 hours → Half Day
    - 3.0 hours → Absent
    """
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


# -------------------------
# Main cleaning function
# -------------------------
def clean_daily_inout30(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout30] Starting - Transaction IN OUT Report")
    print(f"[clean_daily_inout30] Input: {input_path}")
    print(f"[clean_daily_inout30] Output: {output_path}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read the Excel file
    df = pd.read_excel(input_path, dtype=object)
    print(f"[clean_daily_inout30] Raw shape: {df.shape}")
    print(f"[clean_daily_inout30] Columns: {df.columns.tolist()}")

    # Validate required columns
    required_cols = ['Date', 'Ramco EMP ID', 'Contractor Workers Name', 'IN PUNCH', 'OUT PUNCH']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Step 1: Collect all punch records grouped by employee and date
    print("[clean_daily_inout30] Collecting punch records...")
    punch_data = {}  # Key: (ramco_emp_id, date_str), Value: {in_times: [], out_times: [], emp_name: str}

    for idx, row in df.iterrows():
        # Extract values
        date_val = row.get('Date')
        ramco_emp_id = row.get('Ramco EMP ID')
        emp_name = row.get('Contractor Workers Name')
        in_punch = row.get('IN PUNCH')
        out_punch = row.get('OUT PUNCH')

        # Skip if no date or employee ID
        if pd.isna(date_val) or pd.isna(ramco_emp_id):
            continue

        # Parse date
        date_str = parse_date(date_val)
        if not date_str:
            continue

        # Clean employee ID and name
        ramco_emp_id = str(ramco_emp_id).strip()
        emp_name = str(emp_name).strip() if pd.notna(emp_name) else ""

        # Create key for grouping
        key = (ramco_emp_id, date_str)

        # Initialize dict for this employee-date combination
        if key not in punch_data:
            punch_data[key] = {
                'in_times': [],
                'out_times': [],
                'emp_name': emp_name
            }

        # Parse and collect in/out times
        in_time = parse_time(date_str, in_punch, is_checkin=True)
        out_time = parse_time(date_str, out_punch, is_checkin=False)

        if in_time:
            punch_data[key]['in_times'].append(in_time)
        if out_time:
            punch_data[key]['out_times'].append(out_time)

        if (idx + 1) % 100 == 0:
            print(f"[clean_daily_inout30] Processed {idx + 1} raw rows...")

    print(f"[clean_daily_inout30] Found {len(punch_data)} unique employee-date combinations")

    # Step 2: Process each employee-date combination (first IN, last OUT)
    print("[clean_daily_inout30] Processing attendance records (first IN, last OUT)...")
    records = []
    duplicate_count = 0

    for (ramco_emp_id, date_str), data in punch_data.items():
        in_times = data['in_times']
        out_times = data['out_times']
        emp_name = data['emp_name']

        # Count duplicates (more than one punch)
        total_punches = len(in_times) + len(out_times)
        if total_punches > 2:
            duplicate_count += 1

        # Get FIRST IN and LAST OUT
        first_in = min(in_times) if in_times else None
        last_out = max(out_times) if out_times else None

        # Resolve Employee ID in ERPNext using Ramco EMP ID as attendance_device_id
        try:
            emp_code = frappe.db.get_value("Employee", {"attendance_device_id": ramco_emp_id}, "name")
            if not emp_code:
                emp_code = ""
        except Exception:
            emp_code = ""

        # Calculate working hours from first IN to last OUT
        work_hours_decimal = calculate_working_hours(first_in, last_out)
        work_hours_formatted = format_working_hours(work_hours_decimal)

        # Determine status based on working hours
        status = determine_status(work_hours_decimal)

        # Detect shift based on first in-punch time
        shift_code = detect_shift(first_in, last_out)

        # Calculate overtime (9 hour standard, blank if < 1 hour)
        overtime = calculate_overtime(work_hours_decimal)

        # Build record
        record = {
            "Attendance Date": date_str,
            "Employee": emp_code,
            "Employee Name": emp_name,
            "Status": status,
            "In Time": first_in or "",
            "Out Time": last_out or "",
            "Working Hours": work_hours_formatted,
            "Over Time": overtime,
            "Shift": shift_code,
            "Company": company if company else "Vaaman Engineers India Limited",
            "Branch": branch if branch else "",
        }

        records.append(record)

    if duplicate_count > 0:
        print(f"[clean_daily_inout30] ⚠️  Handled {duplicate_count} employee-date(s) with multiple punches (using first IN, last OUT)")

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
            "Please check that the uploaded file matches the selected Branch "
            "and that the file format is correct."
        )

    print(f"[clean_daily_inout30] Total records parsed: {len(df_final)}")

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout30] Saved cleaned file: {output_path}")
    print("[clean_daily_inout30] Done ✅")

    return df_final
