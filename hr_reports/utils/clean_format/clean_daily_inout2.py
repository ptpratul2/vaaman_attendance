# hr_reports/utils/clean_format/clean_daily_inout2.py
# =====================================================
# Cleaning Script for Vaaman Punch Report (Jharsuguda format)
# Features:
# - Converts .xls to .xlsx using xlrd and openpyxl
# - Parses employee blocks with Name and ID
# - Extracts IN (Terminal 1) and OUT (Terminal 2) punches
# - Groups punches by employee and date
# - Calculates working hours (FIRST IN to LAST OUT)
# - Auto-detects shift based on IN time
# - Determines status based on working hours
# - Maps Gate Pass ID to ERPNext Employee
# =====================================================

import os
import re
import pandas as pd
import frappe
import xlrd
from openpyxl import Workbook
import tempfile
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from collections import defaultdict


# =========================
#  .xls -> .xlsx conversion
# =========================
def convert_xls_to_xlsx(xls_path: str) -> str:
    """Convert .xls file to .xlsx using xlrd and openpyxl."""
    print(f"[clean_daily_inout2] Converting .xls to .xlsx: {xls_path}")

    try:
        book = xlrd.open_workbook(xls_path, formatting_info=False)
        sheet = book.sheet_by_index(0)

        print(f"[clean_daily_inout2] XLS dimensions: {sheet.nrows} rows x {sheet.ncols} cols")

        wb = Workbook()
        ws = wb.active

        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                val = sheet.cell_value(r, c)
                # Keep dates as serial numbers for now - we'll parse them later
                ws.cell(row=r + 1, column=c + 1).value = val

        tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
        wb.save(tmp_path)
        print(f"[clean_daily_inout2] Saved temporary .xlsx: {tmp_path}")
        return tmp_path
    except Exception as e:
        print(f"[clean_daily_inout2] XLS conversion failed: {str(e)[:100]}")
        raise


# =========================
#  Helper Functions
# =========================
def parse_excel_serial_date(serial_date: float) -> Optional[datetime]:
    """
    Convert Excel serial date to datetime.
    Excel serial date: number of days since 1899-12-30
    Example: 45963.633... = 2025-11-01 15:11:36
    """
    if not serial_date or pd.isna(serial_date):
        return None

    try:
        # Excel epoch: 1899-12-30
        excel_epoch = datetime(1899, 12, 30)
        dt = excel_epoch + timedelta(days=float(serial_date))
        return dt
    except Exception as e:
        print(f"[clean_daily_inout2] Error parsing serial date {serial_date}: {e}")
        return None


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
        return ""

    _SHIFT_RANGES = {"A": (5, 7), "G": (8, 10), "B": (13, 15), "C": (21, 23)}

    # Exact range match
    for shift, (lo, hi) in _SHIFT_RANGES.items():
        if lo <= hour <= hi:
            return shift

    # Fallback: closest range by nearest endpoint (circular distance)
    best, best_dist = "G", float("inf")
    for shift, (lo, hi) in _SHIFT_RANGES.items():
        d = min(
            min(abs(hour - lo), 24 - abs(hour - lo)),
            min(abs(hour - hi), 24 - abs(hour - hi))
        )
        if d < best_dist:
            best_dist = d
            best = shift
    return best


def calculate_working_hours(intime_str: str, outtime_str: str) -> tuple:
    """
    Calculate working hours from intime and outtime strings.
    Returns: (decimal_hours, total_hours)

    Logic from clean_daily_inout10.py
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
        print(f"[clean_daily_inout2] Error calculating hours: {e}")
        return None, 0.0


def determine_status(working_hours: float, total_hours: float) -> str:
    """
    Determine status based on working hours.
    Logic from clean_daily_inout10.py:
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
    Logic from clean_daily_inout10.py:
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


def extract_employee_info(row_values: List) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract employee name and gate pass ID from employee header row.

    Actual format from file:
    - Col 1: "ABHI MAJHI, ABHI" (employee name)
    - Col 4: "ID:"
    - Col 10: "112222" (gate pass ID)

    Returns: (employee_name, gate_pass_id)
    """
    emp_name = None
    gate_pass_id = None

    # Employee name is in column 1 (index 1)
    if len(row_values) > 1 and pd.notna(row_values[1]):
        name_val = str(row_values[1]).strip()
        if name_val and len(name_val) > 3 and any(c.isalpha() for c in name_val):
            emp_name = name_val

    # Gate pass ID is in column 10 (index 10)
    if len(row_values) > 10 and pd.notna(row_values[10]):
        id_val = row_values[10]
        try:
            # Convert to int to remove decimal point
            gate_pass_id = str(int(float(id_val)))
        except (ValueError, TypeError):
            gate_pass_id = str(id_val).strip()

    return emp_name, gate_pass_id


def is_employee_header_row(row_values: List) -> bool:
    """
    Check if row is an employee header row (contains "ID:" in column 4)
    """
    # Column 4 (index 4) should contain "ID:"
    if len(row_values) > 4 and pd.notna(row_values[4]):
        return str(row_values[4]).strip() == "ID:"
    return False


def is_punch_row(row_values: List) -> bool:
    """
    Check if row is a punch record row.
    Punch rows have:
    - Excel serial date in column 1 (index 1) (numeric > 40000)
    - Terminal info in column 4 (index 4)
    """
    if not row_values or len(row_values) < 5:
        return False

    # Check if column 1 has an Excel serial date
    try:
        val = row_values[1]
        if pd.notna(val) and isinstance(val, (int, float)):
            # Excel dates for 2025 are around 45000+
            if 40000 < float(val) < 50000:
                return True
    except (ValueError, TypeError):
        pass

    return False


def identify_punch_type(row_values: List) -> Optional[str]:
    """
    Identify if punch is IN or OUT based on Terminal number in column 4.
    Terminal 1 = IN
    Terminal 2 = OUT

    Returns: "IN", "OUT", or None
    """
    # Check column 4 (index 4) for terminal information
    if len(row_values) > 4 and pd.notna(row_values[4]):
        terminal_str = str(row_values[4]).strip().lower()
        if "terminal 1" in terminal_str:
            return "IN"
        elif "terminal 2" in terminal_str:
            return "OUT"

    return None


# =========================
#  Main Cleaning Function
# =========================
def clean_daily_inout2(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    """
    Clean Vaaman Punch Report (Jharsuguda format).

    Features:
    - Converts .xls to .xlsx
    - Parses employee blocks (Name + Gate Pass ID)
    - Extracts IN (Terminal 1) and OUT (Terminal 2) punches
    - Groups by employee and date
    - Calculates working hours (FIRST IN to LAST OUT)
    - Auto-detects shift
    - Maps Gate Pass ID to Employee code

    Returns cleaned DataFrame ready for ERPNext import.
    """
    print("=" * 80)
    print("[clean_daily_inout2] Starting Vaaman Punch Report Cleaning")
    print(f"[clean_daily_inout2] Input: {input_path}")
    print(f"[clean_daily_inout2] Output: {output_path}")
    print(f"[clean_daily_inout2] Company: {company}")
    print(f"[clean_daily_inout2] Branch: {branch}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Build employee lookup cache: attendance_device_id -> employee_name
    print("[clean_daily_inout2] Building employee lookup cache...")
    employee_cache = {}
    try:
        employees = frappe.get_all('Employee',
            fields=['name', 'attendance_device_id', 'employee_name']
        )
        total_employees = len(employees)
        employees_with_device_id = 0

        for emp in employees:
            if emp.get('attendance_device_id'):
                try:
                    # Store as string to match with Excel data
                    device_id = str(int(float(emp['attendance_device_id'])))
                    employee_cache[device_id] = emp['name']
                    employees_with_device_id += 1
                except (ValueError, TypeError):
                    continue

        print(f"[clean_daily_inout2] Total employees: {total_employees}")
        print(f"[clean_daily_inout2] Employees with device ID: {employees_with_device_id}")
        print(f"[clean_daily_inout2] Employee cache size: {len(employee_cache)}")

        if len(employee_cache) > 0:
            # Show sample mappings
            sample_items = list(employee_cache.items())[:3]
            print(f"[clean_daily_inout2] Sample gate pass mappings: {sample_items}")

    except Exception as e:
        print(f"[clean_daily_inout2] Warning: Could not load employee cache: {e}")
        print(f"[clean_daily_inout2] Will use gate pass numbers directly")
        employee_cache = {}

    # Step 1: Convert .xls to .xlsx
    working_file = input_path
    temp_created = False

    if input_path.lower().endswith(".xls"):
        xlsx_path = convert_xls_to_xlsx(input_path)
        if xlsx_path:
            working_file = xlsx_path
            temp_created = True
            print(f"[clean_daily_inout2] Using converted .xlsx file: {working_file}")
        else:
            raise ValueError("Failed to convert .xls to .xlsx")

    # Step 2: Read Excel file
    df_raw = pd.read_excel(working_file, engine="openpyxl", header=None)
    print(f"[clean_daily_inout2] Loaded raw DataFrame shape: {df_raw.shape}")

    # Step 3: Parse employee blocks and punch records
    print("[clean_daily_inout2] Parsing employee blocks and punches...")

    current_employee = None
    current_gate_pass = None
    employee_punches = defaultdict(list)  # {(gate_pass, emp_name): [(datetime, punch_type), ...]}

    skipped_header_rows = 0
    employee_blocks_found = 0
    total_punches_parsed = 0

    for idx, row in df_raw.iterrows():
        row_values = row.tolist()

        # Skip first 14 rows (header)
        if idx < 14:
            skipped_header_rows += 1
            continue

        # Check if this is an employee header row
        if is_employee_header_row(row_values):
            emp_name, gate_pass = extract_employee_info(row_values)

            if emp_name and gate_pass:
                current_employee = emp_name
                current_gate_pass = gate_pass
                employee_blocks_found += 1
                print(f"[clean_daily_inout2] Found employee: {emp_name} (Gate Pass: {gate_pass})")
            continue

        # Check if this is a punch row
        if is_punch_row(row_values) and current_employee and current_gate_pass:
            # Parse punch datetime from column 1 (index 1)
            serial_date = row_values[1]
            punch_dt = parse_excel_serial_date(serial_date)

            if punch_dt:
                # Identify punch type (IN/OUT)
                punch_type = identify_punch_type(row_values)

                if punch_type:
                    key = (current_gate_pass, current_employee)
                    employee_punches[key].append((punch_dt, punch_type))
                    total_punches_parsed += 1

    print(f"[clean_daily_inout2] Skipped header rows: {skipped_header_rows}")
    print(f"[clean_daily_inout2] Employee blocks found: {employee_blocks_found}")
    print(f"[clean_daily_inout2] Total punches parsed: {total_punches_parsed}")
    print(f"[clean_daily_inout2] Unique employees with punches: {len(employee_punches)}")

    # Step 4: Group punches by employee and date, calculate working hours
    print("[clean_daily_inout2] Grouping punches by date and calculating working hours...")

    records = []
    not_found_count = 0
    employees_with_multiple_days = 0

    for (gate_pass, emp_name), punches in employee_punches.items():
        # Group punches by date
        punches_by_date = defaultdict(list)  # {date_str: [(datetime, punch_type), ...]}

        for punch_dt, punch_type in punches:
            date_str = punch_dt.strftime("%Y-%m-%d")
            punches_by_date[date_str].append((punch_dt, punch_type))

        if len(punches_by_date) > 1:
            employees_with_multiple_days += 1

        # Process each date
        for att_date, daily_punches in punches_by_date.items():
            # Separate IN and OUT punches
            in_punches = [dt for dt, ptype in daily_punches if ptype == "IN"]
            out_punches = [dt for dt, ptype in daily_punches if ptype == "OUT"]

            # Sort chronologically
            in_punches.sort()
            out_punches.sort()

            # Get FIRST IN and LAST OUT
            first_in = in_punches[0] if in_punches else None
            last_out = out_punches[-1] if out_punches else None

            # Format times
            in_time_str = first_in.strftime("%Y-%m-%d %H:%M:%S") if first_in else ""
            out_time_str = last_out.strftime("%Y-%m-%d %H:%M:%S") if last_out else ""

            # Calculate working hours
            if in_time_str and out_time_str:
                calc_work_hrs, total_hours = calculate_working_hours(in_time_str, out_time_str)

                if calc_work_hrs is not None:
                    work_hrs = calc_work_hrs
                    # Determine status based on calculated hours
                    status = determine_status(work_hrs, total_hours)
                else:
                    work_hrs = ""
                    status = "Absent"
            else:
                # Missing punch time - mark as Absent with blank hours
                work_hrs = ""
                status = "Absent"

            # Auto-detect shift from punch times — blank if Absent
            shift = detect_shift(in_time_str) if status != "Absent" else ""

            # Calculate overtime
            overtime_val = calculate_overtime(work_hrs) if (work_hrs and work_hrs > 0) else ""

            # Look up employee ID from gate pass number
            employee_id = None
            try:
                device_id_str = str(int(float(gate_pass)))
                employee_id = employee_cache.get(device_id_str)

                if employee_id is None:
                    # Not found in ERPNext master - leave Employee cell blank
                    employee_id = ""
                    not_found_count += 1
                    print(f"⚠️  Gate Pass {device_id_str} NOT found in Frappe - Leaving Employee blank (Employee: {emp_name})")
            except (ValueError, TypeError):
                print(f"⚠️  Invalid Gate Pass: {gate_pass} (Employee: {emp_name})")

            # Skip only rows where Gate Pass itself was unparseable (employee_id is still None)
            if employee_id is None:
                print(f"⚠️  SKIPPING row with invalid Gate Pass: {emp_name} on {att_date}")
                continue

            rec = {
                "Attendance Date": att_date,
                "Employee": employee_id,
                "Employee Name": emp_name,
                "Status": status,
                "In Time": in_time_str,
                "Out Time": out_time_str,
                "Company": company if company else "",
                "Branch": branch if branch else "",
                "Working Hours": work_hrs,
                "Shift": shift,
                "Over Time": overtime_val
            }
            records.append(rec)

    print(f"[clean_daily_inout2] Employees with attendance on multiple days: {employees_with_multiple_days}")

    # Step 5: Create final DataFrame
    print(f"[clean_daily_inout2] Total records created: {len(records)}")

    if not records:
        print("[clean_daily_inout2] ❌ ERROR: No records were created!")
        print("[clean_daily_inout2] Debugging info:")
        print(f"  - Employee blocks found: {employee_blocks_found}")
        print(f"  - Total punches parsed: {total_punches_parsed}")
        print(f"  - Unique employees with punches: {len(employee_punches)}")
        raise ValueError("No attendance records parsed from Vaaman Punch Report. Check the file format and parsing logic.")

    df_final = pd.DataFrame.from_records(records)

    # Only drop NaN if we have the columns
    if "Attendance Date" in df_final.columns and "Employee" in df_final.columns:
        df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")

    print(f"[clean_daily_inout2] Built final DataFrame with {len(df_final)} rows")

    if not_found_count > 0:
        print(f"[clean_daily_inout2] ⚠️  Warning: {not_found_count} gate pass IDs not found in Employee master")

    if df_final.empty:
        raise ValueError("No attendance records parsed from Vaaman Punch Report.")

    # Step 6: Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout2] Saved output to: {output_path}")

    # Cleanup temporary file if created
    if temp_created and os.path.exists(working_file):
        os.unlink(working_file)
        print(f"[clean_daily_inout2] Cleaned up temporary file")

    print("[clean_daily_inout2] Done ✅")
    print("=" * 80)

    return df_final
