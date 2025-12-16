# hr_reports/utils/clean_format/clean_daily_inout7.py
# =====================================================
# Cleaning Script for Punch Report (HTML-based .xls format)
# Features:
# - Converts .xls to .xlsx using xlrd and openpyxl
# - Parses HTML table structure from .xls files
# - Groups multiple IN/OUT punches per employee per day
# - Calculates working hours and determines attendance status
# - Auto-detects shift based on punch time
# - Maps Safety Pass No to ERPNext Employee
# =====================================================

import os
import pandas as pd
import frappe
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
from html.parser import HTMLParser


# =========================
#  HTML Parser for Punch Report
# =========================
class PunchReportHTMLParser(HTMLParser):
    """Custom HTML parser to extract table data from HTML-based .xls files"""

    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.in_header = False
        self.current_row = []
        self.rows = []
        self.headers = []
        self.current_data = []

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
        elif tag == 'tr' and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ['td', 'th'] and self.in_row:
            self.in_cell = True
            self.current_data = []
            if tag == 'th':
                self.in_header = True

    def handle_endtag(self, tag):
        if tag == 'table':
            self.in_table = False
        elif tag == 'tr' and self.in_row:
            self.in_row = False
            if self.current_row:
                if self.in_header:
                    self.headers = self.current_row
                    self.in_header = False
                else:
                    self.rows.append(self.current_row)
            self.current_row = []
        elif tag in ['td', 'th'] and self.in_cell:
            self.in_cell = False
            cell_text = ''.join(self.current_data).strip()
            self.current_row.append(cell_text)
            self.current_data = []

    def handle_data(self, data):
        if self.in_cell:
            self.current_data.append(data)


# =========================
#  .xls -> .xlsx conversion
# =========================
def convert_xls_to_xlsx(xls_path: str) -> str:
    """Convert .xls file to .xlsx using xlrd and openpyxl."""
    import xlrd
    from openpyxl import Workbook
    import tempfile

    print(f"[clean_daily_inout7] Converting .xls to .xlsx: {xls_path}")

    try:
        book = xlrd.open_workbook(xls_path, formatting_info=False)
        sheet = book.sheet_by_index(0)
        wb = Workbook()
        ws = wb.active

        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                val = sheet.cell_value(r, c)
                if sheet.cell_type(r, c) == xlrd.XL_CELL_DATE:
                    try:
                        val = xlrd.xldate_as_datetime(val, book.datemode)
                    except Exception:
                        pass
                ws.cell(row=r + 1, column=c + 1).value = val

        tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
        wb.save(tmp_path)
        print(f"[clean_daily_inout7] Saved temporary .xlsx: {tmp_path}")
        return tmp_path
    except Exception as e:
        print(f"[clean_daily_inout7] XLS conversion failed, file might be HTML: {str(e)[:100]}")
        return None  # Return None to indicate HTML parsing should be used


# =========================
#  Helper Functions
# =========================
def parse_html_punch_report(file_path: str) -> pd.DataFrame:
    """Parse HTML-based punch report and return DataFrame"""
    print(f"[clean_daily_inout7] Parsing HTML table from: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    parser = PunchReportHTMLParser()
    parser.feed(content)

    print(f"[clean_daily_inout7] Found {len(parser.rows)} rows in HTML table")
    print(f"[clean_daily_inout7] Headers: {parser.headers}")

    # Create DataFrame from parsed data
    if parser.headers and parser.rows:
        df = pd.DataFrame(parser.rows, columns=parser.headers)
    else:
        # If no headers found, use default column names based on punch report structure
        default_headers = ["Safety Pass No", "Workman Name", "Gatepass Dept.",
                          "Flag", "Shift", "Punch Time/HH:MM:SS",
                          "Reader Department", "Reader Location"]
        df = pd.DataFrame(parser.rows, columns=default_headers[:len(parser.rows[0]) if parser.rows else 0])

    return df


def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract date from filename like 'PunchReport 02.10.2025.xls' -> '2025-10-02' or 'PunchReport 01.12.25.xls' -> '2025-12-01'"""
    import re

    # Pattern 1: DD.MM.YYYY (4-digit year)
    pattern_4digit = r'(\d{2})\.(\d{2})\.(\d{4})'
    match = re.search(pattern_4digit, filename)

    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"

    # Pattern 2: DD.MM.YY (2-digit year)
    pattern_2digit = r'(\d{2})\.(\d{2})\.(\d{2})'
    match = re.search(pattern_2digit, filename)

    if match:
        day, month, year = match.groups()
        # Convert 2-digit year to 4-digit (assumes 2000s)
        full_year = f"20{year}"
        return f"{full_year}-{month}-{day}"

    return None


def detect_shift(punch_time_str: str) -> str:
    """
    Detect shift based on punch time:
    - Shift A (General): 06:00 - 14:00
    - Shift B (Afternoon): 14:00 - 22:00
    - Shift C (Night): 22:00 - 06:00
    - Shift G: Default/General
    """
    if not punch_time_str:
        return "G"

    try:
        # Parse time string (HH:MM:SS)
        time_parts = punch_time_str.split(":")
        hour = int(time_parts[0])

        if 6 <= hour < 14:
            return "A"
        elif 14 <= hour < 22:
            return "B"
        elif hour >= 22 or hour < 6:
            return "C"
        else:
            return "G"
    except Exception:
        return "G"


def parse_time_to_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """Parse date and time strings to datetime object"""
    if not date_str or not time_str:
        return None

    try:
        # Parse date: YYYY-MM-DD
        date_obj = pd.to_datetime(date_str).date()

        # Parse time: HH:MM:SS
        time_parts = time_str.split(":")
        hour = int(time_parts[0])
        minute = int(time_parts[1]) if len(time_parts) > 1 else 0
        second = int(time_parts[2]) if len(time_parts) > 2 else 0

        return datetime.combine(date_obj, datetime.min.time().replace(hour=hour, minute=minute, second=second))
    except Exception as e:
        print(f"[clean_daily_inout7] Error parsing datetime: {e}, date: {date_str}, time: {time_str}")
        return None


def format_datetime_output(dt_obj: Optional[datetime]) -> str:
    """Format datetime to 'YYYY-MM-DD HH:MM:SS AM/PM'"""
    if not dt_obj:
        return ""

    return dt_obj.strftime("%Y-%m-%d %I:%M:%S %p")


def calculate_working_hours(intime_dt: datetime, outtime_dt: datetime, att_date: str) -> tuple:
    """
    Calculate working hours from intime and outtime datetime objects.
    Returns: (decimal_hours, total_hours)

    Logic from clean_daily_inout24.py
    """
    if not intime_dt or not outtime_dt:
        return None, 0.0

    try:
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
        print(f"[clean_daily_inout7] Error calculating hours: {e}")
        return None, 0.0


def determine_status(working_hours: float, total_hours: float) -> str:
    """
    Determine status based on working hours.
    Logic from clean_daily_inout24.py:
    - >= 7 hours: Present
    - >= 4.5 hours: Half Day
    - < 4.5 hours: Absent
    """
    if total_hours >= 7.0:
        return "Present"
    elif total_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def calculate_overtime(working_hours: float) -> str:
    """
    Calculate overtime based on working hours.
    Logic from clean_daily_inout14.py:
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
def clean_daily_inout7(input_path: str, output_path: str, company: str = None, branch: str = None, attendance_date: str = None) -> pd.DataFrame:
    """
    Clean punch report from HTML-based .xls format.

    Features:
    - Converts .xls to .xlsx or parses HTML table structure
    - Groups multiple IN/OUT punches per employee per day
    - Maps Safety Pass No to ERPNext Employee
    - Calculates working hours and overtime
    - Detects shift based on punch time
    - Determines attendance status

    Returns cleaned DataFrame ready for ERPNext import.
    """
    print("=" * 80)
    print("[clean_daily_inout7] Starting Punch Report Cleaning")
    print(f"[clean_daily_inout7] Input: {input_path}")
    print(f"[clean_daily_inout7] Output: {output_path}")
    print(f"[clean_daily_inout7] Company: {company}")
    print(f"[clean_daily_inout7] Branch: {branch}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Extract date from filename if not provided
    if not attendance_date:
        attendance_date = extract_date_from_filename(os.path.basename(input_path))
        if attendance_date:
            print(f"[clean_daily_inout7] Extracted date from filename: {attendance_date}")
        else:
            print("[clean_daily_inout7] WARNING: Could not extract date from filename, using today")
            attendance_date = datetime.now().strftime("%Y-%m-%d")

    # Step 1: Try to convert .xls to .xlsx, if that fails, parse as HTML
    working_file = input_path
    temp_created = False
    df_raw = None

    if input_path.lower().endswith(".xls"):
        xlsx_path = convert_xls_to_xlsx(input_path)
        if xlsx_path:
            working_file = xlsx_path
            temp_created = True
            try:
                df_raw = pd.read_excel(working_file, engine="openpyxl")
                print(f"[clean_daily_inout7] Successfully loaded converted .xlsx file")
            except Exception as e:
                print(f"[clean_daily_inout7] Could not read as Excel, will parse as HTML: {str(e)[:100]}")
                df_raw = None

    # If conversion failed or file is HTML, parse as HTML
    if df_raw is None:
        df_raw = parse_html_punch_report(input_path)

    print(f"[clean_daily_inout7] Loaded DataFrame shape: {df_raw.shape}")
    print(f"[clean_daily_inout7] Columns: {list(df_raw.columns)}")

    # Step 2: Group punches by employee (Safety Pass No)
    print("[clean_daily_inout7] Grouping punches by employee...")

    # Expected columns (with flexible matching) - ORDER MATTERS!
    col_mapping = {}
    for col in df_raw.columns:
        col_lower = str(col).lower().strip()
        # Check gatepass first to avoid matching "pass" in "gatepass dept"
        if 'gatepass' in col_lower and 'dept' in col_lower:
            col_mapping['department'] = col
        elif 'safety' in col_lower and 'pass' in col_lower:
            # Match "Safety Pass No" specifically
            col_mapping['safety_pass'] = col
        elif 'workman' in col_lower or ('name' in col_lower and 'reader' not in col_lower):
            col_mapping['name'] = col
        elif 'flag' in col_lower:
            col_mapping['flag'] = col
        elif 'shift' in col_lower:
            col_mapping['shift'] = col
        elif 'punch' in col_lower and 'time' in col_lower:
            col_mapping['punch_time'] = col
        elif 'reader' in col_lower and 'dept' in col_lower:
            col_mapping['reader_dept'] = col
        elif 'reader' in col_lower and 'location' in col_lower:
            col_mapping['reader_location'] = col

    print(f"[clean_daily_inout7] Column mapping: {col_mapping}")

    # Group punches by Safety Pass No
    employee_punches = defaultdict(list)

    for _, row in df_raw.iterrows():
        safety_pass = str(row.get(col_mapping.get('safety_pass', 'Safety Pass No'), '')).strip()
        emp_name = str(row.get(col_mapping.get('name', 'Workman Name'), '')).strip()
        department = str(row.get(col_mapping.get('department', 'Gatepass Dept.'), '')).strip()
        flag = str(row.get(col_mapping.get('flag', 'Flag'), '')).strip()
        shift_raw = str(row.get(col_mapping.get('shift', 'Shift'), '')).strip()
        punch_time = str(row.get(col_mapping.get('punch_time', 'Punch Time/HH:MM:SS'), '')).strip()

        # Skip empty rows
        if not safety_pass or not emp_name:
            continue

        # Store punch data
        employee_punches[safety_pass].append({
            'name': emp_name,
            'department': department,
            'flag': flag,
            'shift': shift_raw,
            'punch_time': punch_time
        })

    print(f"[clean_daily_inout7] Found {len(employee_punches)} unique employees")

    # Step 3: Process each employee's punches
    records = []
    employees_with_multiple_punches = 0
    employees_with_no_punch = 0
    employees_with_single_punch = 0
    employees_found = 0
    employees_not_found = 0

    for safety_pass, punches in employee_punches.items():
        # Get employee details
        emp_name = punches[0]['name']
        department = punches[0]['department']

        # Lookup Employee in ERPNext
        employee_id = ""
        try:
            # Clean the safety_pass - remove leading/trailing spaces, convert to string
            clean_safety_pass = str(safety_pass).strip()

            # Try exact match first
            employee_id = frappe.db.get_value("Employee", {"attendance_device_id": clean_safety_pass}, "name")

            # If not found, try with TRIM to handle any hidden whitespace
            if not employee_id:
                employee_id = frappe.db.sql("""
                    SELECT name
                    FROM `tabEmployee`
                    WHERE TRIM(attendance_device_id) = %s
                    LIMIT 1
                """, (clean_safety_pass,), as_dict=False)
                employee_id = employee_id[0][0] if employee_id else None

            # If not found, try case-insensitive match
            if not employee_id:
                employee_id = frappe.db.sql("""
                    SELECT name
                    FROM `tabEmployee`
                    WHERE LOWER(TRIM(attendance_device_id)) = LOWER(%s)
                    LIMIT 1
                """, (clean_safety_pass,), as_dict=False)
                employee_id = employee_id[0][0] if employee_id else None

            # If not found and it's purely numeric, try without leading zeros
            if not employee_id and clean_safety_pass.isdigit():
                clean_safety_pass_no_zeros = str(int(clean_safety_pass))
                employee_id = frappe.db.get_value("Employee", {"attendance_device_id": clean_safety_pass_no_zeros}, "name")

            # If still not found and numeric, try with leading zeros (pad to common lengths)
            if not employee_id and clean_safety_pass.isdigit():
                for length in [4, 5, 6, 8]:
                    padded_pass = clean_safety_pass.zfill(length)
                    employee_id = frappe.db.get_value("Employee", {"attendance_device_id": padded_pass}, "name")
                    if employee_id:
                        break

            employee_id = employee_id or ""

            if employee_id:
                employees_found += 1
            else:
                employees_not_found += 1
                print(f"[clean_daily_inout7] WARNING: Employee not found for Safety Pass '{clean_safety_pass}' ({emp_name})")
        except Exception as e:
            employees_not_found += 1
            print(f"[clean_daily_inout7] ERROR: Exception during employee lookup for Safety Pass {safety_pass} ({emp_name}): {str(e)}")

        # Separate IN and OUT punches
        in_punches = []
        out_punches = []
        shift_detected = None

        for punch in punches:
            if punch['punch_time']:
                # Parse punch time to datetime
                punch_dt = parse_time_to_datetime(attendance_date, punch['punch_time'])

                if punch_dt:
                    if punch['flag'] == 'IN':
                        in_punches.append(punch_dt)
                        # Only accept valid shifts: A, B, C, G
                        if not shift_detected and punch['shift'] and punch['shift'] != '-':
                            if punch['shift'].upper() in ['A', 'B', 'C', 'G']:
                                shift_detected = punch['shift'].upper()
                    elif punch['flag'] == 'OUT':
                        out_punches.append(punch_dt)

        # Sort punches chronologically
        in_punches.sort()
        out_punches.sort()

        # Determine first IN and last OUT
        first_in = in_punches[0] if in_punches else None
        last_out = out_punches[-1] if out_punches else None

        # Handle special cases
        if not first_in and not last_out:
            # No punch records - mark as Absent
            status = "Absent"
            working_hours = ""  # Blank instead of 0
            in_time_str = ""
            out_time_str = ""
            employees_with_no_punch += 1
        elif not first_in or not last_out:
            # Only one punch (either IN or OUT only) - mark as Absent
            status = "Absent"
            working_hours = ""  # Blank instead of 0
            in_time_str = format_datetime_output(first_in) if first_in else ""
            out_time_str = format_datetime_output(last_out) if last_out else ""
            employees_with_single_punch += 1
        else:
            # Both IN and OUT exist - calculate working hours
            calc_work_hrs, total_hours = calculate_working_hours(first_in, last_out, attendance_date)

            if calc_work_hrs is not None:
                working_hours = calc_work_hrs
                status = determine_status(working_hours, total_hours)
            else:
                working_hours = ""  # Blank instead of 0
                status = "Absent"

            in_time_str = format_datetime_output(first_in)
            out_time_str = format_datetime_output(last_out)

            if len(in_punches) > 1 or len(out_punches) > 1:
                employees_with_multiple_punches += 1

        # Auto-detect shift if not provided
        if not shift_detected or shift_detected == '-':
            if first_in:
                shift_detected = detect_shift(first_in.strftime("%H:%M:%S"))
            else:
                shift_detected = ""  # Blank instead of "G"

        # Calculate overtime (only if working_hours is a number)
        overtime = calculate_overtime(working_hours) if (working_hours and working_hours > 0) else ""

        # Build record
        rec = {
            "Attendance Date": attendance_date,
            "Employee": employee_id,
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time_str,
            "Out Time": out_time_str,
            "Working Hours": working_hours,
            "Over Time": overtime,
            "Shift": shift_detected,
            "Company": company or "",
            "Branch": branch or ""
        }
        records.append(rec)

    print(f"[clean_daily_inout7] Processing summary:")
    print(f"  - Employees found in ERPNext: {employees_found}")
    print(f"  - Employees NOT found in ERPNext: {employees_not_found}")
    print(f"  - Employees with multiple punches: {employees_with_multiple_punches}")
    print(f"  - Employees with single punch: {employees_with_single_punch}")
    print(f"  - Employees with no punch: {employees_with_no_punch}")

    # Step 4: Create final DataFrame
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
            "Branch"
        ]
    )

    print(f"[clean_daily_inout7] Built final DataFrame with {len(df_final)} rows")

    if df_final.empty:
        raise ValueError("No attendance data parsed from punch report")

    # Step 5: Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout7] Saved cleaned file: {output_path}")

    # Cleanup temporary file
    if temp_created and os.path.exists(working_file):
        os.unlink(working_file)
        print(f"[clean_daily_inout7] Cleaned up temporary file")

    print("[clean_daily_inout7] Done ✅")
    return df_final
