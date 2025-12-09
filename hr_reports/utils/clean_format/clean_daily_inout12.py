# hr_reports/utils/clean_format/clean_daily_inout12.py
# =====================================================
# Cleaning Script for Monthly Punching Report
# Format: Horizontal layout with employee blocks (7 rows per employee)
# Features:
# - Parses employee blocks with Employee ID, VEIL CODE, Employee Name
# - Extracts IN/OUT times for each day (columns 1-30)
# - Maps VEIL CODE to ERPNext Employee
# - Calculates working hours from IN/OUT times
# - Auto-detects shift based on IN time
# - Determines status based on working hours
# =====================================================

import os
import pandas as pd
import frappe
from datetime import datetime, timedelta
from typing import Optional, Dict, List


# =========================
#  Helper Functions
# =========================
def parse_time_value(time_val) -> Optional[datetime]:
    """
    Parse time value which can be:
    - String like "05:50", "13:59"
    - datetime object
    - NaN/None
    Returns datetime.time object or None
    """
    if pd.isna(time_val) or time_val is None:
        return None

    try:
        # If it's already a datetime
        if isinstance(time_val, (datetime, pd.Timestamp)):
            return time_val.time()

        # If it's a string like "05:50"
        time_str = str(time_val).strip()
        if not time_str or time_str == "":
            return None

        # Parse HH:MM format
        parts = time_str.split(":")
        if len(parts) >= 2:
            hour = int(parts[0])
            minute = int(parts[1])
            return datetime.min.time().replace(hour=hour, minute=minute)

        return None
    except Exception as e:
        print(f"[clean_daily_inout12] Error parsing time {time_val}: {e}")
        return None


def format_datetime(date_obj: datetime, time_obj) -> Optional[str]:
    """Format date and time to 'YYYY-MM-DD HH:MM:SS' format for ERPNext"""
    if not date_obj or not time_obj:
        return None

    try:
        if isinstance(time_obj, datetime):
            time_obj = time_obj.time()

        combined = datetime.combine(date_obj.date(), time_obj)
        return combined.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"[clean_daily_inout12] Error formatting datetime: {e}")
        return None


def calculate_working_hours(intime_str: str, outtime_str: str) -> tuple:
    """
    Calculate working hours from intime and outtime strings.
    Returns: (decimal_hours, total_hours)
    """
    if not intime_str or not outtime_str:
        return None, 0.0

    try:
        # Parse datetime strings (format: "YYYY-MM-DD HH:MM:SS")
        intime_dt = datetime.strptime(intime_str, "%Y-%m-%d %H:%M:%S")
        outtime_dt = datetime.strptime(outtime_str, "%Y-%m-%d %H:%M:%S")

        # If outtime is earlier than intime, assume it's next day
        if outtime_dt < intime_dt:
            outtime_dt += timedelta(days=1)

        # Calculate difference
        diff = outtime_dt - intime_dt
        total_seconds = diff.total_seconds()
        hours = total_seconds / 3600

        # Format as decimal (e.g., 8.50)
        decimal_hours = round(hours, 2)

        return decimal_hours, hours
    except Exception as e:
        print(f"[clean_daily_inout12] Error calculating hours: {e}")
        return None, 0.0


def determine_status(working_hours: float, total_hours: float, status_raw: str) -> str:
    """
    Determine status based on working hours and raw status.
    Priority:
    1. If raw status is provided and valid, use it
    2. Otherwise, calculate from working hours:
       - >= 7 hours: Present
       - >= 4.5 hours: Half Day
       - < 4.5 hours: Absent
    """
    # Map raw status codes
    status_map = {
        "P": "Present",
        "AB": "Absent",
        "HD": "Half Day",
        "WO": "Holiday",
        "H": "Holiday",
        "CL": "On Leave",
        "PL": "On Leave",
        "SL": "On Leave",
        "EL": "On Leave",
        "ML": "On Leave",
    }

    # If raw status is provided and valid, use it
    if status_raw and not pd.isna(status_raw):
        status_str = str(status_raw).strip().upper()
        if status_str in status_map:
            return status_map[status_str]

    # Otherwise, calculate from working hours
    if total_hours >= 7.0:
        return "Present"
    elif total_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def detect_shift(in_time: Optional[str]) -> str:
    """
    Detect shift with 1-hour grace period for late arrivals.

    Shift timings with grace:
    - Shift C (Night): 21:00 (9 PM) to 07:00 (7 AM) - includes 1hr grace
    - Shift A (Day): 05:00 (5 AM) to 15:00 (3 PM) - includes 1hr grace
    - Shift B (Evening): 13:00 (1 PM) to 23:00 (11 PM) - includes 1hr grace
    - Shift G (General): Everything else or late entries

    Priority: C > A > B > G (to handle overlaps)
    """
    def get_hour(ts: Optional[str]) -> Optional[int]:
        if not ts or str(ts).strip() == "" or pd.isna(ts):
            return None
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").hour
        except Exception:
            return None

    hour = get_hour(in_time)
    if hour is None:
        return "G"

    # Shift C (Night): 21:00-07:00 (with 1hr grace before/after)
    if hour >= 21 or hour <= 7:
        return "C"
    # Shift A (Day): 05:00-15:00
    elif 7 < hour < 15:
        return "A"
    # Shift B (Evening): 13:00-23:00
    elif 15 <= hour < 21:
        return "B"

    # General shift for anything else
    return "G"


def calculate_overtime(working_hours: float) -> str:
    """
    Calculate overtime based on working hours.
    - All shifts considered as 9 hours
    - OT = Working Hours - 9
    - If OT is negative or less than 1 hour, return blank
    """
    if working_hours <= 0:
        return ""

    shift_hrs = 9  # All shifts are 9 hours
    overtime = round(working_hours - shift_hrs, 2)

    # If OT is negative or less than 1 hour, return blank
    if overtime < 1:
        return ""

    return overtime


# =========================
#  Main Cleaning Function
# =========================
def clean_daily_inout12(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    """
    Clean Monthly Punching Report (horizontal layout with employee blocks).

    File Structure:
    - Header row: Employee ID, VEIL CODE, Employee Name, Contractor Name, Type, 1-30 (days), For Month, For Year, Total, Category
    - Employee blocks (7 rows each):
      1. IN row - punch in times
      2. OUT row - punch out times
      3. STATUS row - attendance status
      4. TOTAL HOURS row
      5. LATE HOURS row
      6. EARLY HOURS row
      7. OT HOURS row

    Returns cleaned DataFrame ready for ERPNext import.
    """
    print("=" * 80)
    print("[clean_daily_inout12] Starting Monthly Punching Report Cleaning")
    print(f"[clean_daily_inout12] Input: {input_path}")
    print(f"[clean_daily_inout12] Output: {output_path}")
    print(f"[clean_daily_inout12] Company: {company}")
    print(f"[clean_daily_inout12] Branch: {branch}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Build employee lookup cache: Device ID -> employee record, VEIL CODE -> employee record
    print("[clean_daily_inout12] Building employee lookup cache...")
    employee_cache_by_veil = {}
    employee_cache_by_device = {}
    try:
        employees = frappe.get_all('Employee',
            fields=['name', 'employee_name', 'attendance_device_id']
        )
        total_employees = len(employees)

        for emp in employees:
            # Cache by Employee ID (VEIL CODE like V46536)
            employee_cache_by_veil[emp['name']] = emp['name']

            # Also cache by Attendance Device ID (like A000044090, B000040098)
            if emp.get('attendance_device_id'):
                device_id = str(emp['attendance_device_id']).strip()
                employee_cache_by_device[device_id] = emp['name']

        print(f"[clean_daily_inout12] Total employees in cache: {total_employees}")
        print(f"[clean_daily_inout12] Employees with Device ID: {len(employee_cache_by_device)}")

    except Exception as e:
        print(f"[clean_daily_inout12] Warning: Could not load employee cache: {e}")
        employee_cache_by_veil = {}
        employee_cache_by_device = {}

    # Read Excel file
    df_raw = pd.read_excel(input_path, engine="openpyxl", header=0)
    print(f"[clean_daily_inout12] Loaded raw DataFrame shape: {df_raw.shape}")

    # Get month and year from first data row
    month = None
    year = None
    if "For Month" in df_raw.columns and "For Year" in df_raw.columns:
        first_row = df_raw.iloc[0]
        month = int(first_row["For Month"]) if pd.notna(first_row["For Month"]) else None
        year = int(first_row["For Year"]) if pd.notna(first_row["For Year"]) else None

    if not month or not year:
        print("[clean_daily_inout12] WARNING: Could not extract month/year from file")
        # Use current month/year as fallback
        today = datetime.today()
        month = today.month
        year = today.year

    print(f"[clean_daily_inout12] Processing month: {year}-{month:02d}")

    # Process employee blocks (every 7 rows)
    records = []
    total_rows = len(df_raw)
    employee_blocks_found = 0
    employees_not_found = 0

    i = 0
    while i < total_rows:
        # Check if this is the start of an employee block (IN row)
        if i + 6 >= total_rows:
            break

        # Get employee info from first row of block
        in_row = df_raw.iloc[i]
        out_row = df_raw.iloc[i + 1]
        status_row = df_raw.iloc[i + 2]
        total_hours_row = df_raw.iloc[i + 3]
        # late_hours_row = df_raw.iloc[i + 4]  # Not used
        # early_hours_row = df_raw.iloc[i + 5]  # Not used
        ot_hours_row = df_raw.iloc[i + 6]

        # Check if this is a valid employee block (Type should be "IN")
        if pd.isna(in_row.get("Type")) or str(in_row.get("Type")).strip().upper() != "IN":
            i += 1
            continue

        employee_id_device = in_row.get("Employee ID")
        veil_code = in_row.get("VEIL CODE")
        emp_name = in_row.get("Employee Name")

        # Skip if essential info is missing
        if pd.isna(employee_id_device) or pd.isna(emp_name):
            i += 7
            continue

        employee_blocks_found += 1

        # Map to Employee using priority order:
        # 1. Primary: Search by Attendance Device ID (from "Employee ID" column)
        # 2. Secondary: Search by VEIL CODE
        # 3. Tertiary: Use VEIL CODE as-is
        employee_id = None
        device_id_str = str(employee_id_device).strip() if pd.notna(employee_id_device) else None
        veil_code_str = str(veil_code).strip() if pd.notna(veil_code) else None

        # PRIMARY: Try to find by Attendance Device ID first (most reliable)
        if device_id_str and device_id_str in employee_cache_by_device:
            employee_id = employee_cache_by_device[device_id_str]
            # print(f"[clean_daily_inout12] ✓ Found employee by Device ID {device_id_str}: {employee_id}")

        # SECONDARY: If not found by device ID, try VEIL CODE
        elif veil_code_str and veil_code_str in employee_cache_by_veil:
            employee_id = employee_cache_by_veil[veil_code_str]
            # print(f"[clean_daily_inout12] ✓ Found employee by VEIL CODE {veil_code_str}: {employee_id}")

        # TERTIARY: Use VEIL CODE as-is (fallback for employees not in cache)
        elif veil_code_str:
            employee_id = veil_code_str
            employees_not_found += 1
            print(f"[clean_daily_inout12] ⚠ Employee not found in cache - using VEIL CODE as-is: {veil_code_str} (Device ID: {device_id_str}) for {emp_name}")

        # LAST RESORT: Use Device ID if nothing else works
        else:
            employee_id = device_id_str
            employees_not_found += 1
            print(f"[clean_daily_inout12] ⚠ No VEIL CODE - using Device ID as-is: {device_id_str} for {emp_name}")

        # Process each day (columns 1-30)
        for day in range(1, 31):
            try:
                # Create date for this day
                att_date = datetime(year, month, day)
            except ValueError:
                # Invalid day for this month (e.g., Feb 30)
                continue

            # Get IN/OUT times for this day
            in_time_val = in_row.get(day)
            out_time_val = out_row.get(day)
            status_val = status_row.get(day)
            # total_hours_val = total_hours_row.get(day)  # Can use this if needed
            ot_hours_val = ot_hours_row.get(day)

            # Skip if no IN or OUT time
            if pd.isna(in_time_val) and pd.isna(out_time_val):
                continue

            # Parse IN and OUT times
            in_time_parsed = parse_time_value(in_time_val)
            out_time_parsed = parse_time_value(out_time_val)

            # Format timestamps
            in_time_str = format_datetime(att_date, in_time_parsed) if in_time_parsed else None
            out_time_str = format_datetime(att_date, out_time_parsed) if out_time_parsed else None

            # Calculate working hours
            if in_time_str and out_time_str:
                calc_work_hrs, total_hours = calculate_working_hours(in_time_str, out_time_str)

                if calc_work_hrs is not None:
                    work_hrs = calc_work_hrs
                    # Determine status based on calculated hours and raw status
                    status = determine_status(work_hrs, total_hours, status_val)
                else:
                    work_hrs = ""
                    status = determine_status(0, 0, status_val)
            else:
                # Missing punch time
                work_hrs = ""
                status = determine_status(0, 0, status_val)

            # Skip if status is Holiday
            if status == "Holiday":
                continue

            # Auto-detect shift
            shift = detect_shift(in_time_str) if in_time_str else ""

            # Calculate overtime
            overtime_val = calculate_overtime(work_hrs) if (work_hrs and work_hrs > 0) else ""

            # Build record
            rec = {
                "Attendance Date": att_date.strftime("%Y-%m-%d"),
                "Employee": employee_id,
                "Employee Name": str(emp_name).strip() if pd.notna(emp_name) else "",
                "Status": status,
                "In Time": in_time_str or "",
                "Out Time": out_time_str or "",
                "Company": company if company else "",
                "Branch": branch if branch else "",
                "Working Hours": work_hrs,
                "Shift": shift,
                "Over Time": overtime_val
            }
            records.append(rec)

        # Move to next employee block (7 rows)
        i += 7

    print(f"[clean_daily_inout12] Employee blocks processed: {employee_blocks_found}")
    print(f"[clean_daily_inout12] Employees not found in ERPNext: {employees_not_found}")
    print(f"[clean_daily_inout12] Total records created: {len(records)}")

    if not records:
        raise ValueError("No attendance records parsed from Monthly Punching Report.")

    # Create final DataFrame
    df_final = pd.DataFrame.from_records(records)

    # Drop rows without employee ID or attendance date
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")
    df_final = df_final[df_final['Employee'] != '']

    print(f"[clean_daily_inout12] Final DataFrame shape: {df_final.shape}")

    if df_final.empty:
        raise ValueError("No valid attendance records after filtering.")

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout12] Saved output to: {output_path}")
    print("[clean_daily_inout12] Done ✅")
    print("=" * 80)

    return df_final
