# clean_daily_inout_matrix_2.py
"""
Cleaner for Monthly Status Report (Basic Work Duration).

Key behaviour:
- Horizontal format with employee blocks
- Each block contains:
  * Department row
  * Employee Code and Name row
  * Status row (P/A/H values for each day)
  * InTime row (time values)
  * OutTime row (time values)
  * Total row (working hours in HH:MM format)
- Dates are in row 5 with format like "21 T", "22 W" (day + weekday abbreviation)
- Maps Employee Code to Employee doctype
- Extracts already-calculated working hours from Total row
- Determines shift based on InTime
- Calculates overtime: OT = Hours - 9 (only shown if >= 1 hour)
"""

import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

import frappe
import pandas as pd


# -------------------------
# Helpers
# -------------------------
def parse_period(df: pd.DataFrame) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Parse the period from the title row.
    Example: "Oct 21 2025  To  Nov 20 2025"
    Returns tuple of (start_date, end_date)
    """
    try:
        # Check first few rows for the period
        for i in range(min(5, len(df))):
            row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()])

            # Match pattern: "Oct 21 2025  To  Nov 20 2025"
            m = re.search(r'(\w+)\s+(\d{1,2})\s+(\d{4})\s+To\s+(\w+)\s+(\d{1,2})\s+(\d{4})', row_text, re.IGNORECASE)
            if m:
                start_month, start_day, start_year, end_month, end_day, end_year = m.groups()
                start_dt = datetime.strptime(f"{start_month} {start_day} {start_year}", "%b %d %Y")
                end_dt = datetime.strptime(f"{end_month} {end_day} {end_year}", "%b %d %Y")
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
    Parse date from cell like "21 T" or "1 St"
    Returns YYYY-MM-DD format
    Handles periods spanning two months (e.g., Oct 21 to Nov 20)
    """
    if pd.isna(cell_value):
        return None

    try:
        # Extract day number from format like "21 T" or "1 St"
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


def parse_working_hours(hours_val) -> float:
    """
    Parse working hours from HH:MM format to decimal hours.
    Example: "12:08" -> 12.13
    """
    if pd.isna(hours_val) or str(hours_val).strip() == "":
        return 0.0

    try:
        time_str = str(hours_val).strip()

        # Handle HH:MM format
        if ":" in time_str:
            parts = time_str.split(":")
            if len(parts) >= 2:
                h = int(parts[0])
                m = int(parts[1])
                return round(h + (m / 60.0), 2)

        return 0.0
    except Exception as e:
        print(f"[parse_working_hours] Error parsing '{hours_val}': {e}")
        return 0.0


def format_working_hours(hours: float) -> str:
    """Convert decimal hours to HH:MM format"""
    if hours <= 0:
        return "00:00"

    h = int(hours)
    m = int((hours - h) * 60)
    return f"{h:02d}:{m:02d}"


def determine_status_from_code(status_code: str, working_hours: float) -> str:
    """
    Determine attendance status from the status code in the file.
    P = Present, A = Absent, H = Half Day
    Falls back to hour-based calculation if code is unclear.
    """
    if not status_code or pd.isna(status_code):
        # Fallback based on hours
        if working_hours >= 7.0:
            return "Present"
        elif working_hours >= 4.5:
            return "Half Day"
        else:
            return "Absent"

    status_code = str(status_code).strip().upper()

    if status_code == "P":
        return "Present"
    elif status_code == "A":
        return "Absent"
    elif status_code == "H":
        return "Half Day"
    else:
        # Fallback based on hours
        if working_hours >= 7.0:
            return "Present"
        elif working_hours >= 4.5:
            return "Half Day"
        else:
            return "Absent"


def detect_shift(in_time: Optional[str]) -> str:
    """
    Detect shift based on InTime.
    - A shift: 05:00 - 07:30
    - G shift: 08:00 - 10:00
    - B shift: 13:00 - 15:00
    - C shift: 18:00 - 23:59
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
        elif 18 <= hour <= 23:
            return "C"
        else:
            # Find nearest shift
            distances = {
                "A": abs(hour - 6),
                "G": abs(hour - 9),
                "B": abs(hour - 14),
                "C": abs(hour - 21)
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


def find_date_row(df: pd.DataFrame) -> int:
    """
    Find the row index that contains the dates.
    Looking for row starting with "Days"
    """
    for idx in range(min(10, len(df))):
        val = df.iloc[idx][0]
        if pd.notna(val) and str(val).strip() == "Days":
            return idx
    return 5  # Default fallback


def find_employee_blocks(df: pd.DataFrame, start_row: int) -> List[Tuple[int, str, str]]:
    """
    Find all employee blocks in the dataframe.
    Returns list of (row_index, emp_code, emp_name) tuples
    """
    employee_blocks = []

    i = start_row
    while i < len(df):
        # Look for "Emp. Code :" pattern
        val = df.iloc[i][0]
        if pd.notna(val) and "Emp. Code" in str(val):
            # Extract employee code from column 3
            emp_code = str(df.iloc[i][3]).strip() if pd.notna(df.iloc[i][3]) else ""

            # Extract employee name from column 12
            emp_name = str(df.iloc[i][12]).strip() if pd.notna(df.iloc[i][12]) else ""

            if emp_code:
                employee_blocks.append((i, emp_code, emp_name))

        i += 1

    return employee_blocks


# -------------------------
# Main cleaning function
# -------------------------
def clean_daily_inout_matrix_2(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout_matrix_2] Starting - Monthly Status Report")
    print(f"[clean_daily_inout_matrix_2] Input: {input_path}")
    print(f"[clean_daily_inout_matrix_2] Output: {output_path}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read the Excel file without headers
    df = pd.read_excel(input_path, header=None)
    print(f"[clean_daily_inout_matrix_2] Raw shape: {df.shape}")

    # Parse period from title row
    start_dt, end_dt = parse_period(df)

    # Extract company name from row 2
    company_name = company
    if not company_name:
        try:
            company_row = df.iloc[2]
            if pd.notna(company_row[4]):
                company_name = str(company_row[4]).strip()
        except:
            pass
    if not company_name:
        company_name = "Vaaman Engineers India Limited"

    # Find date row (row with "Days")
    date_row_idx = find_date_row(df)
    date_row = df.iloc[date_row_idx]

    # Parse all dates from the date row
    date_columns = {}  # col_idx -> date_str
    for col_idx in range(2, len(date_row)):
        date_str = parse_date_from_cell(date_row[col_idx], start_dt, end_dt)
        if date_str:
            date_columns[col_idx] = date_str

    print(f"[clean_daily_inout_matrix_2] Found {len(date_columns)} date columns")

    # Find all employee blocks
    employee_blocks = find_employee_blocks(df, date_row_idx + 1)
    print(f"[clean_daily_inout_matrix_2] Found {len(employee_blocks)} employees")

    # Process each employee block
    records = []

    for emp_row_idx, emp_code, emp_name in employee_blocks:
        try:
            # Resolve Employee ID from Employee Code
            try:
                emp_id = frappe.db.get_value("Employee", {"name": emp_code}, "name")
                if not emp_id:
                    emp_id = emp_code
            except Exception:
                emp_id = emp_code

            # Status row is +1 from employee row
            status_row_idx = emp_row_idx + 1
            # InTime row is +2
            in_row_idx = emp_row_idx + 2
            # OutTime row is +3
            out_row_idx = emp_row_idx + 3
            # Total row is +4
            total_row_idx = emp_row_idx + 4

            if (status_row_idx >= len(df) or in_row_idx >= len(df) or
                out_row_idx >= len(df) or total_row_idx >= len(df)):
                print(f"[clean_daily_inout_matrix_2] Skipping {emp_code} - incomplete block")
                continue

            status_row = df.iloc[status_row_idx]
            in_row = df.iloc[in_row_idx]
            out_row = df.iloc[out_row_idx]
            total_row = df.iloc[total_row_idx]

            # Process each date column
            for col_idx, date_str in date_columns.items():
                status_cell = status_row[col_idx]
                in_cell = in_row[col_idx]
                out_cell = out_row[col_idx]
                total_cell = total_row[col_idx]

                # Parse times
                in_time = parse_time(date_str, in_cell)
                out_time = parse_time(date_str, out_cell)

                # Parse working hours from Total row
                work_hours_decimal = parse_working_hours(total_cell)
                work_hours_formatted = format_working_hours(work_hours_decimal)

                # Skip if no punch data and no hours
                if not in_time and not out_time and work_hours_decimal == 0:
                    continue

                # Determine status
                status = determine_status_from_code(status_cell, work_hours_decimal)

                # Detect shift
                shift_code = detect_shift(in_time)

                # Calculate overtime
                overtime = calculate_overtime(work_hours_decimal)

                # Build record
                record = {
                    "Attendance Date": date_str,
                    "Employee": emp_id,
                    "Employee Name": emp_name,
                    "Status": status,
                    "In Time": in_time or "",
                    "Out Time": out_time or "",
                    "Working Hours": work_hours_formatted,
                    "Over Time": overtime,
                    "Shift": shift_code,
                    "Company": company_name,
                    "Branch": branch if branch else "",
                }

                records.append(record)

        except Exception as e:
            print(f"[clean_daily_inout_matrix_2] Error processing employee {emp_code}: {e}")
            import traceback
            traceback.print_exc()
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

    print(f"[clean_daily_inout_matrix_2] Total records parsed: {len(df_final)}")

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout_matrix_2] Saved cleaned file: {output_path}")
    print("[clean_daily_inout_matrix_2] Done ✅")

    return df_final
