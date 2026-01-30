# hr_reports/utils/clean_format/clean_daily_inout10.py
import os
import pandas as pd
import frappe
from datetime import datetime, timedelta
from typing import Optional, Tuple


# =========================
#  Date Range Helpers
# =========================
def parse_file_date_range(df: pd.DataFrame, date_column: str = "Date") -> Tuple[datetime, datetime]:
    """
    Extract min and max dates from the Date column.
    Returns: (from_date, to_date)
    """
    if date_column not in df.columns:
        raise ValueError(f"Column '{date_column}' not found in file")

    # Parse all dates in the column
    dates = pd.to_datetime(df[date_column], dayfirst=True, errors='coerce')
    valid_dates = dates.dropna()

    if valid_dates.empty:
        # Fallback: use current month
        today = datetime.today()
        from_date = datetime(today.year, today.month, 1)
        if today.month == 12:
            to_date = datetime(today.year, 12, 31)
        else:
            to_date = datetime(today.year, today.month + 1, 1) - timedelta(days=1)
        print(f"[parse_file_date_range] No valid dates found. Using current month: {from_date:%Y-%m-%d} to {to_date:%Y-%m-%d}")
        return from_date, to_date

    from_date = valid_dates.min().to_pydatetime()
    to_date = valid_dates.max().to_pydatetime()

    print(f"[parse_file_date_range] File date range: {from_date:%Y-%m-%d} to {to_date:%Y-%m-%d}")
    return from_date, to_date


def validate_date_range(
    file_from_date: datetime,
    file_to_date: datetime,
    custom_from_date: Optional[str],
    custom_to_date: Optional[str]
) -> Tuple[datetime, datetime]:
    """
    Validate user-selected date range against file date range.
    Args:
        file_from_date: Start date from file
        file_to_date: End date from file
        custom_from_date: User-selected from date (YYYY-MM-DD string or None)
        custom_to_date: User-selected to date (YYYY-MM-DD string or None)
    Returns:
        (validated_from_date, validated_to_date) as datetime objects
    Raises:
        ValueError: If user dates are outside file date range
    """
    # If no custom dates provided, use file dates
    if not custom_from_date or not custom_to_date:
        print(f"[validate_date_range] No custom dates provided. Using file dates: {file_from_date:%Y-%m-%d} to {file_to_date:%Y-%m-%d}")
        return file_from_date, file_to_date

    # Parse custom dates
    try:
        user_from = datetime.strptime(custom_from_date, "%Y-%m-%d")
        user_to = datetime.strptime(custom_to_date, "%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD. Error: {e}")

    # Validate user_from <= user_to
    if user_from > user_to:
        raise ValueError(f"From Date ({custom_from_date}) cannot be after To Date ({custom_to_date})")

    # Validate dates are within file range
    if user_from < file_from_date:
        raise ValueError(
            f"From Date ({custom_from_date}) is before the file's start date ({file_from_date:%Y-%m-%d}). "
            f"File contains data from {file_from_date:%Y-%m-%d} to {file_to_date:%Y-%m-%d}"
        )

    if user_to > file_to_date:
        raise ValueError(
            f"To Date ({custom_to_date}) is after the file's end date ({file_to_date:%Y-%m-%d}). "
            f"File contains data from {file_from_date:%Y-%m-%d} to {file_to_date:%Y-%m-%d}"
        )

    print(f"[validate_date_range] User date range validated: {user_from:%Y-%m-%d} to {user_to:%Y-%m-%d}")
    print(f"[validate_date_range] File date range: {file_from_date:%Y-%m-%d} to {file_to_date:%Y-%m-%d}")

    return user_from, user_to

# =========================
#  .xls -> .xlsx conversion
# =========================
def convert_xls_to_xlsx(xls_path: str) -> str:
    """Convert .xls file to .xlsx using xlrd and openpyxl."""
    import xlrd
    from openpyxl import Workbook
    import tempfile

    print(f"[clean_daily_inout10] Converting .xls to .xlsx: {xls_path}")

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
        print(f"[clean_daily_inout10] Saved temporary .xlsx: {tmp_path}")
        return tmp_path
    except Exception as e:
        print(f"[clean_daily_inout10] XLS conversion failed: {str(e)[:100]}")
        return None


def format_datetime(date_val, time_val):
    """
    Format date and time to 24-hour format: YYYY-MM-DD HH:MM:SS
    """
    if pd.isna(date_val):
        return None

    if not isinstance(date_val, (datetime, pd.Timestamp)):
        date_val = pd.to_datetime(date_val, dayfirst=True, errors="coerce")
    if pd.isna(date_val):
        return None

    if isinstance(time_val, timedelta):
        total_seconds = int(time_val.total_seconds())
        hours = (total_seconds // 3600) % 24
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
    else:
        try:
            t_str = str(time_val).strip()
            if not t_str or t_str.lower() in ["nan", "none"]:
                return None
            parts = t_str.replace(".", ":").split(":")
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
            seconds = int(parts[2]) if len(parts) > 2 else 0
        except Exception:
            return None

    # Keep 24-hour format (no conversion to 12-hour)
    return date_val.strftime("%Y-%m-%d") + f" {hours:02d}:{minutes:02d}:{seconds:02d}"

def calculate_working_hours(intime_str: str, outtime_str: str) -> tuple:
    """
    Calculate working hours from intime and outtime strings.
    Returns: (display_hours, total_seconds)
    - display_hours: HH.MM format (e.g., 8.14 = 8 hours 14 minutes)
    - total_seconds: raw seconds for status/overtime calculations
    """
    if not intime_str or not outtime_str:
        return None, 0

    try:
        # Parse datetime strings (format: "YYYY-MM-DD HH:MM:SS")
        intime_dt = datetime.strptime(intime_str, "%Y-%m-%d %H:%M:%S")
        outtime_dt = datetime.strptime(outtime_str, "%Y-%m-%d %H:%M:%S")

        # If outtime is earlier than intime, assume it's next day
        if outtime_dt <= intime_dt:
            outtime_dt += timedelta(days=1)

        # Calculate difference
        diff = outtime_dt - intime_dt
        total_seconds = diff.total_seconds()

        # Format as HH.MM (e.g., 8.14 = 8 hours 14 minutes)
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        display_hours = float(f"{hours}.{minutes:02d}")

        return display_hours, total_seconds
    except Exception as e:
        print(f"[clean_daily_inout10] Error calculating hours: {e}")
        return None, 0

def _to_float_workhrs(time_val):
    if not time_val or str(time_val).lower() in ["nan", "none"]:
        return 0.0

    # Handle datetime directly
    if isinstance(time_val, (datetime, pd.Timestamp)):
        h = time_val.hour
        m = time_val.minute
        return float(f"{h:02d}.{m:02d}")

    # Handle string like "08:15" or "1900-01-24 08:15:13"
    try:
        dt = pd.to_datetime(time_val, errors="coerce")
        if pd.isna(dt):
            return 0.0
        h = dt.hour
        m = dt.minute
        return float(f"{h:02d}.{m:02d}")
    except Exception:
        return 0.0




def determine_status(total_seconds: float) -> str:
    """
    Determine status based on working hours.
    - >= 7 hours: Present
    - >= 4.5 hours: Half Day
    - < 4.5 hours: Absent
    """
    total_hours = total_seconds / 3600 if total_seconds > 0 else 0
    if total_hours >= 7.0:
        return "Present"
    elif total_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"

def detect_shift(in_time: Optional[str], out_time: Optional[str]) -> str:
    """
    Detect shift based on In Time hour.
    Shift A: 5:00 AM - 7:00 AM (hours 5, 6, 7)
    Shift G: 8:00 AM - 10:00 AM (hours 8, 9, 10)
    Shift B: 1:00 PM - 3:00 PM (hours 13, 14, 15)
    Shift C: 9:00 PM - 11:00 PM (hours 21, 22, 23)
    """
    def get_hour(ts: Optional[str]) -> Optional[int]:
        if not ts or str(ts).strip() == "" or pd.isna(ts):
            return None
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").hour
        except Exception:
            return None

    in_hour = get_hour(in_time)
    out_hour = get_hour(out_time)

    hour = in_hour if in_hour is not None else out_hour
    if hour is None:
        return ""

    if 5 <= hour <= 7:        # 5 AM - 7 AM
        return "A"
    elif 8 <= hour <= 10:     # 8 AM - 10 AM
        return "G"
    elif 13 <= hour <= 15:    # 1 PM - 3 PM
        return "B"
    elif 21 <= hour <= 23:    # 9 PM - 11 PM
        return "C"
    return ""

def calculate_overtime(total_seconds: float, shift: str) -> str:
    """
    Calculate overtime based on working hours.
    - Shift hours: 8 hours + 1 hour grace = 9 hours threshold
    - OT = Working Hours - 9
    - If OT is negative or less than 1 hour, return blank
    - Returns HH.MM format (e.g., 1.30 = 1 hour 30 minutes)
    """
    if total_seconds <= 0:
        return ""

    total_hours = total_seconds / 3600
    shift_hrs = {"A": 8, "B": 8, "C": 8, "G": 8}.get(str(shift).upper(), 8)
    ot_hours = total_hours - shift_hrs - 1  # 1 hour grace

    if ot_hours < 1:
        return ""

    # Convert to HH.MM format
    ot_hrs = int(ot_hours)
    ot_mins = int((ot_hours - ot_hrs) * 60)
    return float(f"{ot_hrs}.{ot_mins:02d}")


def parse_time_to_datetime(date_str, time_str):
    """Parse date and time string to datetime object"""
    try:
        if pd.isna(date_str) or not date_str:
            return None
        if pd.isna(time_str) or not time_str:
            return None

        date_obj = pd.to_datetime(date_str).date()
        time_parts = str(time_str).strip().split()
        if len(time_parts) < 2:
            return None

        time_part = time_parts[1]  # HH:MM:SS
        h, m, s = map(int, time_part.split(':'))

        return datetime.combine(date_obj, datetime.min.time().replace(hour=h, minute=m, second=s))
    except Exception:
        return None


def merge_first_in_last_out(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge multiple punches per Employee+Date into single record.
    Uses First In Time and Last Out Time.
    """
    if df.empty:
        return df

    print(f"[clean_daily_inout10] Merge: Processing {len(df)} records...")

    # Parse In/Out times to datetime for comparison
    df['_in_dt'] = df.apply(
        lambda r: parse_time_to_datetime(r['Attendance Date'], r['In Time']) if r['In Time'] else None,
        axis=1
    )
    df['_out_dt'] = df.apply(
        lambda r: parse_time_to_datetime(r['Attendance Date'], r['Out Time']) if r['Out Time'] else None,
        axis=1
    )

    merged_rows = []

    for (emp, date), group in df.groupby(['Employee', 'Attendance Date']):
        if not emp or not date:
            continue

        # Get First In (earliest) and Last Out (latest)
        in_times = group['_in_dt'].dropna()
        out_times = group['_out_dt'].dropna()

        first_in = in_times.min() if not in_times.empty else None
        last_out = out_times.max() if not out_times.empty else None

        # Handle overnight: if last_out <= first_in, add 1 day to out
        if first_in and last_out and last_out <= first_in:
            last_out = last_out + timedelta(days=1)

        # Calculate working hours
        work_hrs_display = ""
        total_seconds = 0
        if first_in and last_out:
            total_seconds = (last_out - first_in).total_seconds()
            if total_seconds > 0:
                hours = int(total_seconds // 3600)
                minutes = int((total_seconds % 3600) // 60)
                work_hrs_display = float(f"{hours}.{minutes:02d}")

        # Calculate status
        status = determine_status(total_seconds)

        # Format times for output
        first_in_str = first_in.strftime("%Y-%m-%d %H:%M:%S") if first_in else ""
        last_out_str = last_out.strftime("%Y-%m-%d %H:%M:%S") if last_out else ""

        # Detect shift from first in time
        shift = detect_shift(first_in_str, last_out_str)

        # Calculate overtime
        overtime_val = calculate_overtime(total_seconds, shift)

        merged_row = {
            'Attendance Date': date,
            'Employee': emp,
            'Employee Name': group.iloc[0]['Employee Name'],
            'Status': status,
            'In Time': first_in_str,
            'Out Time': last_out_str,
            'Company': group.iloc[0]['Company'],
            'Branch': group.iloc[0]['Branch'],
            'Working Hours': work_hrs_display,
            'Shift': shift,
            'Over Time': overtime_val
        }
        merged_rows.append(merged_row)

        print(f"[clean_daily_inout10] Merged {len(group)} punches for {emp} on {date} -> First In: {first_in_str}, Last Out: {last_out_str}, Hours: {work_hrs_display}")

    merged_df = pd.DataFrame(merged_rows)
    print(f"[clean_daily_inout10] Merge complete: {len(df)} records -> {len(merged_df)} merged records")

    return merged_df


def clean_daily_inout10(
    input_path: str,
    output_path: str,
    company: str = None,
    branch: str = None,
    custom_from_date: str = None,
    custom_to_date: str = None
) -> pd.DataFrame:
    """
    Process attendance file and filter by date range.

    Args:
        input_path: Path to input Excel file
        output_path: Path to save cleaned Excel file
        company: Company name
        branch: Branch name
        custom_from_date: User-selected from date (YYYY-MM-DD format, optional)
        custom_to_date: User-selected to date (YYYY-MM-DD format, optional)

    Returns:
        DataFrame with cleaned attendance records
    """
    print("="*80)
    print("[clean_daily_inout10] Starting")
    print(f"Input: {input_path}, Output: {output_path}")
    print(f"Company: {company}, Branch: {branch}")
    print(f"Custom From Date: {custom_from_date}")
    print(f"Custom To Date: {custom_to_date}")
    print("="*80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Build employee lookup cache: attendance_device_id -> employee_name
    # Also build reverse cache: employee_name -> employee_name (for direct ID lookup)
    print("[clean_daily_inout10] Building employee lookup cache...")
    employee_cache = {}
    employee_id_cache = {}  # New: Maps Employee ID (V43437) to itself
    try:
        # Try without status filter first to get all employees
        employees = frappe.get_all('Employee',
            fields=['name', 'attendance_device_id', 'employee_name']
        )
        total_employees = len(employees)
        employees_with_device_id = 0

        for emp in employees:
            # Cache by Employee ID (e.g., V43437 -> V43437)
            employee_id_cache[emp['name']] = emp['name']

            if emp.get('attendance_device_id'):
                try:
                    # Store as string to match with Excel data
                    device_id = str(int(float(emp['attendance_device_id'])))
                    employee_cache[device_id] = emp['name']
                    employees_with_device_id += 1
                except (ValueError, TypeError):
                    continue

        print(f"[clean_daily_inout10] Total employees: {total_employees}")
        print(f"[clean_daily_inout10] Employees with device ID: {employees_with_device_id}")
        print(f"[clean_daily_inout10] Employee cache size: {len(employee_cache)}")
        print(f"[clean_daily_inout10] Employee ID cache size: {len(employee_id_cache)}")

        if len(employee_cache) > 0:
            # Show sample mappings
            sample_items = list(employee_cache.items())[:3]
            print(f"[clean_daily_inout10] Sample gate pass mappings: {sample_items}")

    except Exception as e:
        print(f"[clean_daily_inout10] Warning: Could not load employee cache: {e}")
        print(f"[clean_daily_inout10] Will use gate pass numbers directly")
        employee_cache = {}
        employee_id_cache = {}

    # Handle .xls to .xlsx conversion if needed
    working_file = input_path
    temp_created = False

    if input_path.lower().endswith(".xls"):
        xlsx_path = convert_xls_to_xlsx(input_path)
        if xlsx_path:
            working_file = xlsx_path
            temp_created = True
            print(f"[clean_daily_inout10] Using converted .xlsx file: {working_file}")
        else:
            print(f"[clean_daily_inout10] Conversion failed, will try reading as-is")

    df_raw = pd.read_excel(working_file, engine="openpyxl")
    print(f"[clean_daily_inout10] Loaded raw DataFrame shape: {df_raw.shape}")

    # Only require Date, Employee ID, Employee Name, and punch times
    # We will calculate Status, AWH, OT, SHIFT ourselves
    required_cols = ["Date", "Employee ID", "Employee Name", "IN Time Punch", "OUT Time Punch"]
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    # 1) Parse file date range from Date column
    file_from_date, file_to_date = parse_file_date_range(df_raw, "Date")

    # 2) Validate and get final date range to process
    filter_from_date, filter_to_date = validate_date_range(
        file_from_date, file_to_date, custom_from_date, custom_to_date
    )
    print(f"[clean_daily_inout10] Processing attendance for: {filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d}")

    records = []
    not_found_count = 0
    skipped_date_count = 0
    for _, row in df_raw.iterrows():
        attendance_device_id = row.get("Employee ID") if pd.notna(row.get("Employee ID")) else None
        emp_name = str(row.get("Employee Name")).strip() if pd.notna(row.get("Employee Name")) else None
        att_date = row.get("Date")
        time_in = row.get("IN Time Punch")
        time_out = row.get("OUT Time Punch")

        # Skip empty rows (no punch data)
        if pd.isna(time_in) and pd.isna(time_out):
            continue

        # Look up employee ID from attendance device ID (gate pass number) or Employee ID
        employee_id = None
        if attendance_device_id:
            # First, try as numeric gate pass number
            try:
                device_id_str = str(int(float(attendance_device_id)))
                employee_id = employee_cache.get(device_id_str)
                if employee_id:
                    # Found by gate pass number
                    pass
                else:
                    # Not found by gate pass - use as-is
                    employee_id = device_id_str
                    not_found_count += 1
                    print(f"⚠️  Gate Pass {device_id_str} NOT found in Frappe - Using gate pass as Employee ID (Employee: {emp_name})")
            except (ValueError, TypeError):
                # Not a numeric gate pass - maybe it's an Employee ID (like V43437)?
                attendance_device_id_str = str(attendance_device_id).strip()
                if attendance_device_id_str in employee_id_cache:
                    # Found by Employee ID!
                    employee_id = employee_id_cache[attendance_device_id_str]
                    print(f"✓ Using Frappe Employee ID directly: {employee_id} ({emp_name})")
                else:
                    print(f"⚠️  Invalid Employee ID: {attendance_device_id} (Employee: {emp_name}) - Not found in Frappe")
        else:
            print(f"⚠️  No Employee ID in source Excel for: {emp_name} on {att_date}")

        # Format IN and OUT times
        in_time_fmt = format_datetime(att_date, time_in)
        out_time_fmt = format_datetime(att_date, time_out)

        # Calculate working hours from punch times
        if in_time_fmt and out_time_fmt:
            work_hrs, total_seconds = calculate_working_hours(in_time_fmt, out_time_fmt)

            if work_hrs is not None:
                # Determine status based on total seconds
                status = determine_status(total_seconds)
            else:
                work_hrs = ""
                total_seconds = 0
                status = "Absent"
        else:
            # Missing punch time - mark as Absent with blank hours
            work_hrs = ""
            total_seconds = 0
            status = "Absent"

        # Auto-detect shift from punch times
        shift = detect_shift(in_time_fmt, out_time_fmt)

        # Calculate overtime
        overtime_val = calculate_overtime(total_seconds, shift)

        # Skip rows without Employee ID - cannot import without it
        if not employee_id:
            print(f"⚠️  SKIPPING row without Employee ID: {emp_name} on {att_date}")
            continue

        # Parse attendance date and filter by date range
        parsed_date = pd.to_datetime(att_date, dayfirst=True).strftime("%Y-%m-%d") if pd.notna(att_date) else ""

        # Filter by date range
        if parsed_date:
            try:
                current_date = datetime.strptime(parsed_date, "%Y-%m-%d")
                if current_date < filter_from_date or current_date > filter_to_date:
                    skipped_date_count += 1
                    continue  # Skip dates outside user-selected range
            except Exception:
                pass  # If date parsing fails, include the record

        rec = {
            "Attendance Date": parsed_date,
            "Employee": employee_id,
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time_fmt,
            "Out Time": out_time_fmt,
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": work_hrs,
            "Shift": shift,
            "Over Time": overtime_val
        }
        records.append(rec)

    df_final = pd.DataFrame.from_records(records)
    print(f"[clean_daily_inout10] Built DataFrame with {len(df_final)} rows (before merge)")

    # Drop records with missing Employee ID
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")
    df_final = df_final[df_final['Employee'] != '']
    print(f"[clean_daily_inout10] After dropping invalid records: {len(df_final)} rows")

    if not_found_count > 0:
        print(f"[clean_daily_inout10] ⚠️  Warning: {not_found_count} attendance device IDs not found in Employee master")

    if skipped_date_count > 0:
        print(f"[clean_daily_inout10] Skipped {skipped_date_count} records outside date range ({filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d})")

    # Merge multiple punches per Employee+Date into First In, Last Out
    if not df_final.empty:
        df_final = merge_first_in_last_out(df_final)

    if df_final.empty:
        raise ValueError(
            f"No attendance records found for the selected date range "
            f"({filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d}). "
            "Please check that the uploaded file matches the selected Branch "
            "and date range, and that the file format is correct."
        )

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout10] Saved output to: {output_path}")
    print(f"[clean_daily_inout10] Processed {len(df_final)} attendance records for date range: {filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d}")

    # Cleanup temporary file if created
    if temp_created and os.path.exists(working_file):
        os.unlink(working_file)
        print(f"[clean_daily_inout10] Cleaned up temporary file")

    print("[clean_daily_inout10] Done ✅")

    return df_final
