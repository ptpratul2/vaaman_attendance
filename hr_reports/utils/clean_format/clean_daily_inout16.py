# clean_daily_inout16.py
"""
Cleaner for rptDAttendanceReg Report (Polycab format).

Key behaviour:
- Employee blocks with 12 rows per employee
- Each block contains:
  * Row 0: Employee info (User ID, Name)
  * Row 1: Blank
  * Row 2: Dates (columns 3+)
  * Row 3: Shift codes (not used - shift detected from IN time)
  * Row 4: First IN times
  * Row 5: Last OUT times
  * Row 6-7: Gross Time/Overtime (not used - recalculated)
  * Row 8: Stat1 (Status codes: PR/WO/AB/PH/PL/LI/UL/TR/EO)
  * Row 9-11: Stat2 and blank rows
- Maps User ID to Employee using attendance_device_id
- Calculates working hours from First IN to Last OUT
- Determines status based on working hours (like clean_daily_inout_matrix):
  * >= 7.0 hours → Present
  * >= 4.5 hours → Half Day
  * < 4.5 hours → Absent
  * Special codes (WO/PH/PL/LI) → handled separately
- Detects shift based on First IN time (A/G/B/C)
- Handles month transitions (e.g., October 26 → November 25, December 20 → January 5)
"""

import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import frappe
import pandas as pd


# -------------------------
# Helpers
# -------------------------
def parse_period(df: pd.DataFrame) -> Optional[datetime]:
    """
    Parse the period from the title row.
    Example: "Custom Attendance Register From 26/10/2025 To 25/11/2025"
    Returns the month/year from the start date
    """
    try:
        # Check first few rows for the period
        for i in range(min(5, len(df))):
            row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()])

            # Match pattern: "From DD/MM/YYYY To DD/MM/YYYY"
            m = re.search(r'From\s+(\d{1,2})/(\d{1,2})/(\d{4})', row_text, re.IGNORECASE)
            if m:
                day, month, year = m.groups()
                dt = datetime(int(year), int(month), int(day))
                print(f"[parse_period] Found period: {dt:%Y-%m-%d}")
                return dt

        # Fallback to current date
        today = datetime.today()
        print(f"[parse_period] Period not found, using today: {today:%Y-%m}")
        return today
    except Exception as e:
        print(f"[parse_period] Error: {e}")
        return datetime.today()


def parse_date_from_cell(cell_value, month_dt: datetime, prev_day: int = 0) -> tuple[Optional[str], int]:
    """
    Parse date from cell like "26\\nSun" or "1\\nSat"
    Returns (YYYY-MM-DD format, day_number)
    Handles month transitions by detecting when day goes backward (e.g., 31 -> 1)
    """
    if pd.isna(cell_value):
        return None, 0

    try:
        # Extract day number from format like "26\nSun" or just "26"
        text = str(cell_value).strip()
        m = re.match(r'(\d{1,2})', text)
        if m:
            day = int(m.group(1))

            # Detect month transition: if current day < previous day, we've moved to next month
            if prev_day > 0 and day < prev_day and prev_day > 20:
                # We've transitioned to next month
                next_month = month_dt.replace(day=1) + timedelta(days=32)
                month_dt = next_month.replace(day=1)

            # Compose date with the month/year
            try:
                date_obj = datetime(month_dt.year, month_dt.month, day)
                return date_obj.strftime("%Y-%m-%d"), day
            except ValueError:
                # Invalid day for this month, try next month
                next_month = month_dt.replace(day=1) + timedelta(days=32)
                date_obj = datetime(next_month.year, next_month.month, day)
                return date_obj.strftime("%Y-%m-%d"), day
        return None, 0
    except Exception as e:
        print(f"[parse_date_from_cell] Error parsing '{cell_value}': {e}")
        return None, 0


def parse_time(date_str: str, time_val) -> Optional[str]:
    """
    Parse time value and combine with date.
    Returns 'YYYY-MM-DD HH:MM:SS'
    """
    if pd.isna(time_val) or str(time_val).strip() == "":
        return None

    try:
        time_str = str(time_val).strip()

        # Handle HH:MM format
        if ":" in time_str:
            parts = time_str.split(":")
            if len(parts) >= 2:
                h = int(parts[0])
                m = int(parts[1])
                s = int(parts[2]) if len(parts) > 2 else 0
                return f"{date_str} {h:02d}:{m:02d}:{s:02d}"

        return None
    except Exception as e:
        print(f"[parse_time] Error parsing time '{time_val}': {e}")
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


def determine_status_from_hours(working_hours: float, stat_code: str) -> str:
    """
    Determine attendance status based on working hours and stat code.
    Similar to clean_daily_inout_matrix logic:
    - WO/PH/PL/LI status codes → treated as is
    - >= 7.0 hours → "Present"
    - >= 4.5 hours → "Half Day"
    - < 4.5 hours → "Absent"
    """
    # Handle special status codes first
    code = str(stat_code).strip().upper() if pd.notna(stat_code) else ""

    status_map = {
        "WO": "Present",    # Weekly Off
        "PH": "On Leave",   # Public Holiday
        "PL": "On Leave",   # Paid Leave
        "LI": "On Leave",   # Leave
        "UL": "Absent",     # Unpaid Leave
    }

    if code in status_map:
        return status_map[code]

    # For PR/AB/TR/EO and others, determine by working hours
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def detect_shift_from_time(in_time: Optional[str]) -> str:
    """
    Detect shift based on First IN time (similar to clean_daily_inout_matrix).
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


def normalize_user_id(user_id: str) -> str:
    """
    Normalize User ID by removing leading zeros.
    Examples:
    - 00916201 → 916201
    - 0913206 → 913206
    - 916201 → 916201 (no change)
    """
    if not user_id:
        return ""

    # Strip leading zeros
    normalized = user_id.lstrip('0')

    # If all zeros, return single zero
    if not normalized:
        return "0"

    return normalized


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
def clean_daily_inout16(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout16] Starting - rptDAttendanceReg Report (Polycab)")
    print(f"[clean_daily_inout16] Input: {input_path}")
    print(f"[clean_daily_inout16] Output: {output_path}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read the Excel file without headers
    df = pd.read_excel(input_path, header=None)
    print(f"[clean_daily_inout16] Raw shape: {df.shape}")

    # Parse period from title row
    month_dt = parse_period(df)

    # Find all employee rows (rows with numeric Sr No in column 1)
    employee_rows = []
    for idx in range(len(df)):
        val = df.iloc[idx][1]  # Column 1 has Sr No
        # Check if it's a number (Sr No)
        if pd.notna(val) and str(val).strip().isdigit():
            employee_rows.append(idx)

    print(f"[clean_daily_inout16] Found {len(employee_rows)} employees")

    # Process each employee block
    records = []

    for emp_row_idx in employee_rows:
        try:
            # Extract only necessary employee info
            emp_row = df.iloc[emp_row_idx]
            user_id_raw = str(emp_row[2]).strip() if pd.notna(emp_row[2]) else ""  # User ID from file
            emp_name = str(emp_row[4]).strip() if pd.notna(emp_row[4]) else ""  # Employee Name

            if not user_id_raw:
                continue

            # Normalize User ID by removing leading zeros (00916201 → 916201)
            user_id = normalize_user_id(user_id_raw)

            # Resolve Employee ID from User ID using attendance_device_id
            # Keep blank if not found (let Data Import handle the error)
            try:
                emp_code = frappe.db.get_value("Employee", {"attendance_device_id": user_id}, "name")
                if not emp_code:
                    emp_code = ""  # Blank, not skip
                    print(f"[clean_daily_inout16] Warning: No Employee found for User ID {user_id} (raw: {user_id_raw}) - keeping blank")
            except Exception as e:
                emp_code = ""  # Blank on error
                print(f"[clean_daily_inout16] Error looking up User ID {user_id}: {e} - keeping blank")

            # Row positions relative to employee row
            # Row +2 has dates
            # Row +4 has First IN times
            # Row +5 has Last OUT times
            # Row +8 has Stat1 (status code)

            date_row_idx = emp_row_idx + 2
            in_row_idx = emp_row_idx + 4
            out_row_idx = emp_row_idx + 5
            stat1_row_idx = emp_row_idx + 8

            # Check if all required rows exist
            max_row_idx = max(date_row_idx, in_row_idx, out_row_idx, stat1_row_idx)
            if max_row_idx >= len(df):
                print(f"[clean_daily_inout16] Skipping {user_id} - incomplete block")
                continue

            date_row = df.iloc[date_row_idx]
            in_row = df.iloc[in_row_idx]
            out_row = df.iloc[out_row_idx]
            stat1_row = df.iloc[stat1_row_idx]

            # Track current month for date transitions
            current_month_dt = month_dt
            prev_day = 0

            # Process each date column (starting from column 3)
            for col_idx in range(3, len(date_row)):
                date_cell = date_row[col_idx]
                in_cell = in_row[col_idx]
                out_cell = out_row[col_idx]
                stat1_cell = stat1_row[col_idx]

                # Parse date with month transition handling
                date_str, current_day = parse_date_from_cell(date_cell, current_month_dt, prev_day)
                if not date_str:
                    continue

                # Update month if we transitioned
                if prev_day > 0 and current_day < prev_day and prev_day > 20:
                    current_month_dt = current_month_dt.replace(day=1) + timedelta(days=32)
                    current_month_dt = current_month_dt.replace(day=1)

                prev_day = current_day

                # Parse IN and OUT times
                in_time = parse_time(date_str, in_cell)
                out_time = parse_time(date_str, out_cell)

                # Calculate working hours from IN and OUT times
                work_hours_decimal = calculate_working_hours(in_time, out_time)
                work_hours_formatted = format_working_hours(work_hours_decimal)

                # Determine status based on working hours
                # If no punch or 0 hours → Absent
                if not in_time and not out_time:
                    status = "Absent"
                elif work_hours_decimal == 0.0:
                    status = "Absent"
                elif work_hours_decimal >= 7.0:
                    status = "Present"
                elif work_hours_decimal >= 4.5:
                    status = "Half Day"
                else:
                    status = "Absent"

                # Detect shift from IN time (like clean_daily_inout_matrix)
                shift_code = detect_shift_from_time(in_time)

                # Calculate overtime
                overtime = calculate_overtime(work_hours_decimal)

                # Build record
                record = {
                    "Attendance Date": date_str,
                    "Employee": emp_code,
                    "Employee Name": emp_name,
                    "Status": status,
                    "In Time": in_time or "",
                    "Out Time": out_time or "",
                    "Working Hours": work_hours_formatted,
                    "Over Time": overtime,
                    "Shift": shift_code,
                    "Company": company if company else "Polycab India Ltd",
                    "Branch": branch if branch else "",
                }

                records.append(record)

        except Exception as e:
            print(f"[clean_daily_inout16] Error processing employee at row {emp_row_idx}: {e}")
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

    print(f"[clean_daily_inout16] Total records parsed: {len(df_final)}")

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout16] Saved cleaned file: {output_path}")
    print("[clean_daily_inout16] Done ✅")

    return df_final
