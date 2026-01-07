# clean_daily_inout_matrix.py
"""
Cleaner for Matrix Report (Horizontal format).

Key behaviour:
- Horizontal matrix format with employee blocks (8 rows per employee)
- Each block contains:
  * Row 0: Employee summary (User ID, Name, Department, Designation, Branch, Totals)
  * Row 2: Dates (columns 3+)
  * Row 4: First IN times
  * Row 5: Last OUT times
- Maps User ID to Employee using attendance_device_id
- Calculates working hours from First IN to Last OUT
- Determines status based on working hours:
  * >= 7.0 hours → Present
  * >= 4.5 hours → Half Day
  * < 4.5 hours → Absent
- Calculates overtime: OT = Hours - 9 (only shown if >= 1 hour)
- Detects shift based on First IN time
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
def parse_period(df: pd.DataFrame) -> tuple:
    """
    Parse the period from the title row.
    Example: "Custom Attendance Register From ‎21/10/2025‎ To ‎20/11/2025"
    Returns tuple of (start_date, end_date)
    """
    try:
        # Check first few rows for the period
        for i in range(min(5, len(df))):
            row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()])

            # Match pattern: "From DD/MM/YYYY To DD/MM/YYYY"
            m = re.search(r'From\s+.*?(\d{1,2})/(\d{1,2})/(\d{4}).*?To\s+.*?(\d{1,2})/(\d{1,2})/(\d{4})', row_text, re.IGNORECASE)
            if m:
                start_day, start_month, start_year, end_day, end_month, end_year = m.groups()
                start_dt = datetime(int(start_year), int(start_month), int(start_day))
                end_dt = datetime(int(end_year), int(end_month), int(end_day))
                print(f"[parse_period] Found period: {start_dt:%Y-%m-%d} to {end_dt:%Y-%m-%d}")
                return start_dt, end_dt

        # Fallback to current date
        today = datetime.today()
        print(f"[parse_period] Period not found, using today")
        return today, today
    except Exception as e:
        print(f"[parse_period] Error: {e}")
        today = datetime.today()
        return today, today


def parse_date_from_cell(cell_value, start_dt: datetime, end_dt: datetime) -> Optional[str]:
    """
    Parse date from cell like "21\\nTue" or "1\\nSat"
    Returns YYYY-MM-DD format
    Handles periods spanning two months (e.g., Nov 21 to Dec 20)
    """
    if pd.isna(cell_value):
        return None

    try:
        # Extract day number from format like "21\nTue" or just "21"
        text = str(cell_value).strip()
        m = re.match(r'(\d{1,2})', text)
        if m:
            day = int(m.group(1))

            # Determine which month this day belongs to
            # If day >= start_day, it's in the start month
            # Otherwise, it's in the end month
            if day >= start_dt.day:
                # This day belongs to the start month
                try:
                    date_obj = datetime(start_dt.year, start_dt.month, day)
                    return date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            # This day belongs to the end month
            try:
                date_obj = datetime(end_dt.year, end_dt.month, day)
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return None
    except Exception as e:
        print(f"[parse_date_from_cell] Error parsing '{cell_value}': {e}")
        return None


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


def determine_status(working_hours: float) -> str:
    """
    Determine attendance status based on working hours.
    - >= 7.0 hours → "Present"
    - >= 4.5 hours → "Half Day"
    - < 4.5 hours → "Absent"
    """
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def detect_shift(in_time: Optional[str]) -> str:
    """
    Detect shift based on First IN time.
    - A shift: 05:00 - 07:00
    - G shift: 08:00 - 10:00
    - B shift: 13:00 - 15:00
    - C shift: 21:00 - 23:00
    """
    if not in_time or str(in_time).strip() == "":
        return "G"

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
        return "G"


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
def clean_daily_inout_matrix(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout_matrix] Starting - Matrix Report")
    print(f"[clean_daily_inout_matrix] Input: {input_path}")
    print(f"[clean_daily_inout_matrix] Output: {output_path}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read the Excel file without headers
    df = pd.read_excel(input_path, header=None)
    print(f"[clean_daily_inout_matrix] Raw shape: {df.shape}")

    # Parse period from title row
    start_dt, end_dt = parse_period(df)

    # Find all employee rows (rows with User IDs starting with 'A' in column 2)
    employee_rows = []
    for idx in range(len(df)):
        val = df.iloc[idx][2]
        if pd.notna(val) and isinstance(val, str) and val.startswith('A'):
            employee_rows.append(idx)

    print(f"[clean_daily_inout_matrix] Found {len(employee_rows)} employees")

    # Process each employee block
    records = []

    for emp_row_idx in employee_rows:
        try:
            # Extract employee info from summary row
            emp_row = df.iloc[emp_row_idx]
            user_id = str(emp_row[2]).strip() if pd.notna(emp_row[2]) else ""
            emp_name = str(emp_row[5]).strip() if pd.notna(emp_row[5]) else ""
            department = str(emp_row[10]).strip() if pd.notna(emp_row[10]) else ""
            designation = str(emp_row[13]).strip() if pd.notna(emp_row[13]) else ""
            emp_branch = str(emp_row[16]).strip() if pd.notna(emp_row[16]) else ""

            if not user_id:
                continue

            # Resolve Employee ID from User ID
            try:
                emp_code = frappe.db.get_value("Employee", {"attendance_device_id": user_id}, "name")
                if not emp_code:
                    emp_code = ""
            except Exception:
                emp_code = ""

            # Date row is +2 from employee row
            date_row_idx = emp_row_idx + 2
            # First IN row is +4
            in_row_idx = emp_row_idx + 4
            # Last OUT row is +5
            out_row_idx = emp_row_idx + 5

            if date_row_idx >= len(df) or in_row_idx >= len(df) or out_row_idx >= len(df):
                print(f"[clean_daily_inout_matrix] Skipping {user_id} - incomplete block")
                continue

            date_row = df.iloc[date_row_idx]
            in_row = df.iloc[in_row_idx]
            out_row = df.iloc[out_row_idx]

            # Process each date column (starting from column 3)
            for col_idx in range(3, len(date_row)):
                date_cell = date_row[col_idx]
                in_cell = in_row[col_idx]
                out_cell = out_row[col_idx]

                # Parse date
                date_str = parse_date_from_cell(date_cell, start_dt, end_dt)
                if not date_str:
                    continue

                # Parse IN and OUT times
                in_time = parse_time(date_str, in_cell)
                out_time = parse_time(date_str, out_cell)

                # If no punch data, mark as Absent with blank fields
                if not in_time and not out_time:
                    record = {
                        "Attendance Date": date_str,
                        "Employee": emp_code,
                        "Employee Name": emp_name,
                        "Status": "Absent",
                        "In Time": "",
                        "Out Time": "",
                        "Working Hours": "",
                        "Over Time": "",
                        "Shift": "",
                        "Company": company if company else "Vaaman Engineers India Limited",
                        "Branch": branch if branch else emp_branch,
                    }
                    records.append(record)
                    continue

                # Calculate working hours
                work_hours_decimal = calculate_working_hours(in_time, out_time)
                work_hours_formatted = format_working_hours(work_hours_decimal)

                # Determine status
                status = determine_status(work_hours_decimal)

                # Detect shift
                shift_code = detect_shift(in_time)

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
                    "Company": company if company else "Vaaman Engineers India Limited",
                    "Branch": branch if branch else emp_branch,
                }

                records.append(record)

        except Exception as e:
            print(f"[clean_daily_inout_matrix] Error processing employee at row {emp_row_idx}: {e}")
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

    print(f"[clean_daily_inout_matrix] Total records parsed: {len(df_final)}")

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout_matrix] Saved cleaned file: {output_path}")
    print("[clean_daily_inout_matrix] Done ✅")

    return df_final
