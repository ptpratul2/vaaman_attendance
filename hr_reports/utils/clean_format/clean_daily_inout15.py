# hr_reports/utils/clean_format/clean_daily_inout15.py
# =====================================================
# Cleaning Script for Scrum Report (Horizontal Muster Report)
# Features:
# - Parses horizontal layout with dates as columns
# - Extracts In Time and Out Time from multi-line cells
# - CALCULATES Working Hours from In/Out times
# - CALCULATES Status based on working hours thresholds
# - CALCULATES OT from working hours
# - CALCULATES Shift from In Time
# - Maps ID No (gate pass) to ERPNext Employee
# - Formats output for ERPNext Attendance import
# =====================================================

import os
import re
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import frappe
import pandas as pd
import xlrd
from openpyxl import Workbook


# =========================
#  .xls -> .xlsx conversion
# =========================
def convert_xls_to_xlsx(xls_path: str) -> str:
    """Convert .xls file to .xlsx using xlrd and openpyxl."""
    book = xlrd.open_workbook(xls_path, formatting_info=False)
    sheet = book.sheet_by_index(0)

    wb = Workbook()
    ws = wb.active

    for r in range(sheet.nrows):
        for c in range(sheet.ncols):
            cell_value = sheet.cell_value(r, c)
            if sheet.cell_type(r, c) == xlrd.XL_CELL_DATE:
                try:
                    cell_value = xlrd.xldate_as_datetime(cell_value, book.datemode)
                except Exception:
                    pass
            ws.cell(row=r + 1, column=c + 1).value = cell_value

    temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
    wb.save(temp_xlsx)
    return temp_xlsx


# =========================
#  Helper Functions
# =========================
def parse_report_period(df: pd.DataFrame, max_rows: int = 5) -> datetime:
    """
    Parse month/year from header like:
    'Horizontal Muster Report From 01/11/2025 To 30/11/2025'
    Returns the first date (start of period)
    """
    for i in range(min(max_rows, len(df))):
        row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()])

        # Pattern: "From DD/MM/YYYY To DD/MM/YYYY"
        m = re.search(
            r'from\s+(\d{1,2}[/-]\d{1,2}[/-]\d{4})\s+to\s+(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
            row_text,
            re.IGNORECASE,
        )

        if m:
            date_str = m.group(1).replace("-", "/")
            try:
                dt = datetime.strptime(date_str, "%d/%m/%Y")
                return dt
            except Exception:
                pass

    # Fallback: use current month
    today = datetime.today()
    return datetime(year=today.year, month=today.month, day=1)


def detect_header_row(df: pd.DataFrame, start: int = 0, max_check: int = 20) -> Optional[int]:
    """
    Find the header row containing: 'Workmen', 'ID No' (gate pass)
    and day numbers (1.0, 2.0, ... 30.0 or 31.0)
    """
    for r in range(start, min(len(df), max_check)):
        row_vals = df.iloc[r].tolist()

        # Convert to lowercase strings for matching, but keep original for debugging
        row_vals_lower = [str(val).lower().strip() if pd.notna(val) else "" for val in row_vals]

        # Check for 'Workmen' column
        has_workmen = any('workmen' in val or 'workman' in val for val in row_vals_lower)

        # Check for 'ID No' column (handle variations: "ID No", "IDNo", "ID No.", "Id No")
        has_id_no = any(
            'id' in val and 'no' in val.replace('.', '')  # Handle "ID No", "ID No.", etc.
            for val in row_vals_lower
        )

        # Check if there are day numbers (numeric values like 1, 2, 3... or 1.0, 2.0, 3.0)
        day_count = 0
        for val in row_vals:
            if pd.notna(val):
                # Check if it's a number between 1 and 31
                try:
                    num = float(val)
                    if 1 <= num <= 31:
                        day_count += 1
                except (ValueError, TypeError):
                    pass

        # Header row must have both required columns AND day numbers
        if has_workmen and has_id_no and day_count >= 5:
            return r

    return None


def build_date_map(header_row: pd.Series, month_dt: datetime) -> Dict[int, str]:
    """
    Map column index → 'YYYY-MM-DD' for columns containing day numbers (1.0, 2.0, ..., 30.0)
    """
    date_map: Dict[int, str] = {}

    for idx, cell in enumerate(header_row.tolist()):
        if pd.isna(cell):
            continue

        # Try to parse as number first (handles both float 1.0 and int 1)
        try:
            num = float(cell)
            if 1 <= num <= 31 and num == int(num):  # Must be whole number
                day = int(num)
                dt = datetime(year=month_dt.year, month=month_dt.month, day=day)
                date_map[idx] = dt.strftime("%Y-%m-%d")
                continue
        except (ValueError, TypeError):
            pass

        # Fallback: try as string pattern "1.0", "2.0", etc.
        s = str(cell).strip()
        m = re.match(r'^(\d{1,2})\.0$', s)

        if m:
            try:
                day = int(m.group(1))
                dt = datetime(year=month_dt.year, month=month_dt.month, day=day)
                date_map[idx] = dt.strftime("%Y-%m-%d")
            except Exception:
                continue

    return date_map


def parse_multiline_cell(cell_value) -> Tuple[str, str]:
    """
    Parse multi-line cell content to extract ONLY In Time and Out Time:
    Line 0: Status (ignored - we calculate this)
    Line 1: In Time (extract this)
    Line 2: Out Time (extract this)
    Line 3: ManHours (ignored - we calculate this)
    Line 4: OT (ignored - we calculate this)
    Line 5: ManDays (ignored)

    Returns: (in_time, out_time)
    """
    if pd.isna(cell_value) or str(cell_value).strip() == "":
        return ("", "")

    # Split by newlines
    lines = str(cell_value).split('\n')

    in_time = lines[1].strip() if len(lines) > 1 else ""
    out_time = lines[2].strip() if len(lines) > 2 else ""

    return (in_time, out_time)


def format_timestamp(date_str: str, time_str: str) -> Optional[str]:
    """
    Combine date + time into 'YYYY-MM-DD HH:MM:SS' format (24-hour).
    Handles 12-hour format like "01:26 PM"
    """
    if not date_str or not time_str or str(time_str).strip() == "":
        return None

    try:
        time_str = str(time_str).strip()

        # Try parsing with AM/PM (12-hour format)
        try:
            time_obj = datetime.strptime(time_str, "%I:%M %p")
        except ValueError:
            # Try without AM/PM (24-hour format)
            try:
                time_obj = datetime.strptime(time_str, "%H:%M")
            except ValueError:
                # Try with seconds
                try:
                    time_obj = datetime.strptime(time_str, "%H:%M:%S")
                except ValueError:
                    return None

        # Combine date + time
        full_datetime = datetime.strptime(date_str, "%Y-%m-%d").replace(
            hour=time_obj.hour,
            minute=time_obj.minute,
            second=time_obj.second
        )

        # Return in 24-hour format
        return full_datetime.strftime("%Y-%m-%d %H:%M:%S")

    except Exception:
        return None


def calculate_working_hours(in_time_str: str, out_time_str: str) -> Tuple[Optional[float], float]:
    """
    Calculate working hours from in_time and out_time.
    Handles overnight shifts (if out_time < in_time, add 1 day to out_time).

    Returns: (decimal_hours, total_hours_float)
    - decimal_hours: rounded to 2 decimals (e.g., 8.50)
    - total_hours_float: exact hours for status calculation
    """
    if not in_time_str or not out_time_str:
        return None, 0.0

    try:
        # Parse datetime strings (format: "YYYY-MM-DD HH:MM:SS")
        in_dt = datetime.strptime(in_time_str, "%Y-%m-%d %H:%M:%S")
        out_dt = datetime.strptime(out_time_str, "%Y-%m-%d %H:%M:%S")

        # If out_time is earlier than in_time, assume it's next day (overnight shift)
        if out_dt < in_dt:
            out_dt += timedelta(days=1)

        # Calculate difference
        diff = out_dt - in_dt
        total_seconds = diff.total_seconds()
        hours = total_seconds / 3600

        # Format as decimal (e.g., 8.50)
        decimal_hours = round(hours, 2)

        return decimal_hours, hours

    except Exception as e:
        print(f"[calculate_working_hours] Error: {e}")
        return None, 0.0


def determine_status(working_hours: float) -> str:
    """
    Determine status based on working hours thresholds:
    - >= 7 hours: Present
    - >= 4.5 hours: Half Day
    - < 4.5 hours: Absent
    """
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def calculate_overtime(working_hours: float) -> str:
    """
    Calculate overtime from working hours.
    Standard shift hours: 9 hours
    OT = Working Hours - 9
    If OT < 1 hour, return blank
    """
    if not working_hours or working_hours <= 0:
        return ""

    shift_hours = 9
    overtime = round(working_hours - shift_hours, 2)

    # Return blank if less than 1 hour
    if overtime < 1:
        return ""

    return overtime


def detect_shift(in_time: Optional[str]) -> str:
    """
    Auto-detect shift based on In Time:
    - Shift C (Night): 22:00 - 06:00
    - Shift A (Day): 06:00 - 14:00
    - Shift B (Evening): 14:00 - 22:00
    - Shift G (General): Default
    """
    if not in_time or str(in_time).strip() == "":
        return "G"

    try:
        # Parse datetime string
        dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        hour = dt.hour

        # Shift detection with priority: C > A > B > G
        if hour >= 22 or hour < 6:
            return "C"
        elif 6 <= hour < 14:
            return "A"
        elif 14 <= hour < 22:
            return "B"
        else:
            return "G"

    except Exception:
        return "G"


def extract_employee_info(row: pd.Series, header_row: pd.Series, debug: bool = False) -> Tuple[str, str]:
    """
    Extract employee information from row:
    - Workmen (employee name)
    - ID No (gate pass / attendance device ID)

    Returns: (workmen_name, id_no)
    """
    workmen_name = ""
    id_no = ""

    # Find column indices from header
    header_vals = header_row.tolist()
    header_lower = [str(val).lower().strip() if pd.notna(val) else "" for val in header_vals]

    workmen_col = -1
    id_col = -1

    for idx, header_val in enumerate(header_lower):
        if 'workmen' in header_val or 'workman' in header_val:
            workmen_col = idx
            workmen_name = str(row.iloc[idx]).strip() if pd.notna(row.iloc[idx]) else ""
        elif ('id' in header_val and 'no' in header_val.replace('.', '')) or 'idno' in header_val:
            id_col = idx
            id_no_val = row.iloc[idx]
            if pd.notna(id_no_val):
                # Convert to string, remove decimals if present
                try:
                    id_no = str(int(float(id_no_val)))
                except (ValueError, TypeError):
                    id_no = str(id_no_val).strip()

    if debug:
        print(f"[extract_employee_info] Header: {header_vals[:10]}")
        print(f"[extract_employee_info] Workmen col={workmen_col}, ID col={id_col}")
        print(f"[extract_employee_info] Extracted: name='{workmen_name}', id='{id_no}'")

    return (workmen_name, id_no)


# =========================
#  Main Cleaning Function
# =========================
def clean_daily_inout15(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    """
    Clean Scrum Report (Horizontal Muster Report).

    Features:
    - Parses horizontal layout with dates as columns
    - Extracts In/Out times from multi-line cells
    - CALCULATES Working Hours from In/Out times
    - CALCULATES Status from working hours (>= 7h = Present, >= 4.5h = Half Day, < 4.5h = Absent)
    - CALCULATES OT from working hours (OT = hours - 9, blank if < 1h)
    - CALCULATES Shift from In Time
    - Maps ID No (gate pass) to ERPNext Employee
    - Skips empty cells and invalid records
    - Formats output for ERPNext import

    Returns cleaned DataFrame ready for ERPNext Attendance import.
    """
    # Silent processing - minimal logs

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Step 1: Convert .xls to .xlsx if needed
    working_file = input_path
    temp_created = False

    if input_path.lower().endswith(".xls"):
        working_file = convert_xls_to_xlsx(input_path)
        temp_created = True

    # Read Excel file
    df_raw = pd.read_excel(working_file, header=None, engine="openpyxl")

    # Parse report period (month/year)
    month_dt = parse_report_period(df_raw, max_rows=5)

    # Detect header row
    header_row_idx = detect_header_row(df_raw, start=0, max_check=20)
    if header_row_idx is None:
        raise ValueError("Could not find header row with 'Workmen', 'ID No' and day numbers")

    # Build date map
    header_row = df_raw.iloc[header_row_idx]
    date_map = build_date_map(header_row, month_dt)
    if not date_map:
        raise ValueError("Could not map day columns from header row")

    # Build employee cache
    employee_cache = {}
    try:
        employees = frappe.get_all('Employee',
            fields=['name', 'employee_name', 'attendance_device_id']
        )
        for emp in employees:
            if emp.get('attendance_device_id'):
                device_id = str(emp['attendance_device_id']).strip()
                employee_cache[device_id] = emp['name']
    except Exception:
        employee_cache = {}

    # Process employee rows

    records = []
    start_row = header_row_idx + 1
    not_found_count = 0
    empty_cell_count = 0
    processed_rows = 0
    missing_ids = []  # Track missing employee IDs for summary

    for row_idx in range(start_row, len(df_raw)):
        row = df_raw.iloc[row_idx]

        # Extract employee info (Workmen name, ID No)
        workmen_name, id_no = extract_employee_info(row, header_row, debug=False)

        # Skip if no employee name or ID (invalid row)
        if not workmen_name or not id_no:
            continue

        processed_rows += 1

        # Look up Employee using cache (fast dictionary lookup)
        employee_id = None
        if id_no:
            employee_id = employee_cache.get(id_no)

            if not employee_id:
                not_found_count += 1
                if len(missing_ids) < 10:  # Store first 10 missing IDs
                    missing_ids.append(f"{id_no} ({workmen_name})")
                # Don't skip - still create record for Data Import to validate
                # Data Import will show error and user can add employee then re-import
                employee_id = id_no  # Use ID No as placeholder

        # Process each day column
        for col_idx, date_str in date_map.items():
            if col_idx >= len(row):
                continue

            cell_value = row.iloc[col_idx]

            # Parse multi-line cell content (extract In Time and Out Time only)
            in_time_raw, out_time_raw = parse_multiline_cell(cell_value)

            # Skip if both in_time and out_time are empty (empty cell)
            if not in_time_raw and not out_time_raw:
                empty_cell_count += 1
                continue

            # Format timestamps
            in_time = format_timestamp(date_str, in_time_raw)
            out_time = format_timestamp(date_str, out_time_raw)

            # Calculate working hours from In/Out times
            if in_time and out_time:
                work_hrs_decimal, work_hrs_float = calculate_working_hours(in_time, out_time)

                if work_hrs_decimal is not None:
                    # Calculate status based on working hours
                    status = determine_status(work_hrs_float)
                    working_hours = work_hrs_decimal
                else:
                    # If calculation failed, mark as Absent
                    status = "Absent"
                    working_hours = ""
            else:
                # Missing punch time - mark as Absent with blank hours
                status = "Absent"
                working_hours = ""

            # Calculate shift from In Time
            shift = detect_shift(in_time)

            # Calculate overtime from working hours
            overtime = calculate_overtime(working_hours) if (working_hours and working_hours > 0) else ""

            # Build record
            rec = {
                "Attendance Date": date_str,
                "Employee": employee_id,
                "Employee Name": workmen_name,
                "Status": status,
                "In Time": in_time if in_time else "",
                "Out Time": out_time if out_time else "",
                "Working Hours": working_hours if working_hours else "",
                "Over Time": overtime,
                "Shift": shift,
                "Company": company if company else "",
                "Branch": branch if branch else "",
            }
            records.append(rec)

    # Show warnings for missing employees only
    if not_found_count > 0:
        print(f"⚠️  WARNING: {not_found_count} employees not found in ERPNext")
        if len(missing_ids) > 0:
            print(f"Missing IDs: {', '.join(missing_ids[:5])}")
            if not_found_count > 5:
                print(f"... and {not_found_count - 5} more")
        print(f"Records created with placeholder IDs. Add employees then re-import.\n")

    # Step 8: Create final DataFrame
    if not records:
        error_msg = f"❌ No attendance records found in Scrum Report.\n\n"
        error_msg += f"Possible reasons:\n"
        error_msg += f"  - File has no attendance data (all cells empty)\n"
        error_msg += f"  - File structure doesn't match expected format\n"
        error_msg += f"  - Header row not detected correctly\n\n"
        error_msg += f"Summary:\n"
        error_msg += f"  - Employee rows in file: {processed_rows}\n"
        error_msg += f"  - Empty attendance cells: {empty_cell_count}\n"
        raise ValueError(error_msg)

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

    # Drop rows with missing critical fields
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")

    if df_final.empty:
        raise ValueError("No attendance records parsed after filtering.")

    # Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)

    # Cleanup temporary file
    if temp_created and os.path.exists(working_file):
        try:
            os.unlink(working_file)
        except Exception:
            pass

    return df_final
