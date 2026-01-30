# hr_reports/utils/clean_format/clean_daily_inout13.py
import os
import tempfile
import pandas as pd
import frappe
import xlrd
from openpyxl import Workbook
from datetime import datetime, timedelta
from typing import Optional, Tuple


# =========================
#  Date Range Helpers
# =========================
def parse_file_date_range(df: pd.DataFrame, date_column: str = "Attand Date") -> Tuple[datetime, datetime]:
    """
    Extract min and max dates from the date column.
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
# .xls -> .xlsx conversion
# =========================
def convert_xls_to_xlsx(xls_path: str) -> str:
    print(f"[convert_xls_to_xlsx] Converting .xls to .xlsx: {xls_path}")
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
    print(f"[convert_xls_to_xlsx] Saved temporary .xlsx: {temp_xlsx}")
    return temp_xlsx

def parse_date_dd_mm_yyyy(date_val):
    """Parse date string in DD/MM/YYYY format"""
    if pd.isna(date_val):
        return None
    
    # If already a datetime object, return it
    if isinstance(date_val, (datetime, pd.Timestamp)):
        return date_val
    
    try:
        date_str = str(date_val).strip()
        if not date_str or date_str.lower() in ["nan", "none", ""]:
            return None
        
        # Try DD/MM/YYYY format first
        try:
            return pd.to_datetime(date_str, format="%d/%m/%Y", errors="raise")
        except (ValueError, TypeError):
            pass
        
        # Try DD-MM-YYYY format
        try:
            return pd.to_datetime(date_str, format="%d-%m-%Y", errors="raise")
        except (ValueError, TypeError):
            pass
        
        # Try other common formats
        try:
            return pd.to_datetime(date_str, format="%Y-%m-%d", errors="raise")
        except (ValueError, TypeError):
            pass
        
        # Last resort: let pandas infer (but this might cause the issue)
        return pd.to_datetime(date_str, errors="coerce")
    except Exception:
        return None

def format_datetime(date_val, time_val):
    """
    Format date and time to 24-hour format: YYYY-MM-DD HH:MM:SS
    """
    if pd.isna(date_val):
        return None

    # Parse date using DD/MM/YYYY format
    if not isinstance(date_val, (datetime, pd.Timestamp)):
        date_val = parse_date_dd_mm_yyyy(date_val)
    if pd.isna(date_val) or date_val is None:
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

def _to_float_workhrs(time_str):
    if not time_str or str(time_str).lower() in ["nan", "none", "bf"]:
        return 0.0
    try:
        parts = str(time_str).split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return round(h + m/60 + s/3600, 2)
    except Exception:
        return 0.0


def _seconds_to_decimal_hours(total_seconds: float) -> float:
    """Convert seconds → HH.MM format (e.g., 8 hours 14 minutes = 8.14)"""
    if not total_seconds or total_seconds <= 0:
        return 0.0
    total_seconds = int(total_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    # Return as HH.MM (e.g., 8.14 means 8 hours 14 minutes)
    return float(f"{hours}.{minutes:02d}")


def parse_time_to_datetime(date_str, time_str):
    """Parse date and time string to datetime object"""
    try:
        if pd.isna(date_str) or not date_str:
            return None
        if pd.isna(time_str) or not time_str:
            return None

        # Parse date
        date_obj = pd.to_datetime(date_str).date()

        # Parse time: YYYY-MM-DD HH:MM:SS format
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

    print(f"[clean_daily_inout13] Merge: Processing {len(df)} records...")

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
                work_hrs_display = _seconds_to_decimal_hours(total_seconds)

        # Calculate status
        work_hrs_for_calc = total_seconds / 3600 if total_seconds > 0 else 0
        if not first_in or not last_out:
            status = "Absent"
        elif work_hrs_for_calc >= 7.0:
            status = "Present"
        elif work_hrs_for_calc >= 4.5:
            status = "Half Day"
        else:
            status = "Absent"

        # Format times for output
        first_in_str = first_in.strftime("%Y-%m-%d %H:%M:%S") if first_in else ""
        last_out_str = last_out.strftime("%Y-%m-%d %H:%M:%S") if last_out else ""

        # Detect shift from first in time
        shift = detect_shift(first_in_str, last_out_str)

        # Calculate overtime
        overtime_val = ""
        if work_hrs_for_calc > 0:
            shift_hrs = {"A": 8, "B": 8, "C": 8, "G": 8}.get(str(shift).upper(), 8)
            ot_hours = work_hrs_for_calc - shift_hrs - 1  # 1 hour grace
            if ot_hours > 0:
                ot_hrs = int(ot_hours)
                ot_mins = int((ot_hours - ot_hrs) * 60)
                overtime_val = float(f"{ot_hrs}.{ot_mins:02d}")

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

        print(f"[clean_daily_inout13] Merged {len(group)} punches for {emp} on {date} -> First In: {first_in_str}, Last Out: {last_out_str}, Hours: {work_hrs_display}")

    merged_df = pd.DataFrame(merged_rows)
    print(f"[clean_daily_inout13] Merge complete: {len(df)} records -> {len(merged_df)} merged records")

    return merged_df

def map_status(raw_status) -> str:
    s = "" if pd.isna(raw_status) else str(raw_status).strip()
    mapping = {
        "P": "Present", "POW": "Present",
        "A": "Absent", "AB": "Absent", "O": "Absent",
        "WO": "Holiday", "H": "Holiday",
        "CL": "On Leave", "PL": "On Leave", "SL": "On Leave", "EL": "On Leave", "AP": "On Leave", 
        "RL": "On Leave", "LWP": "On Leave", "SDL": "On Leave", "QL": "On Leave",
        "TU": "On Leave", "CO": "On Leave", "TR": "On Leave", "OH": "On Leave",
        "ML": "On Leave",
        "MIS": "Half Day", "HD": "Half Day", "HALF": "Half Day",
        "E": "Work From Home"
    }
    return mapping.get(s, s if s else "Absent")


def _calculate_overtime(work_hrs_str, shift):
    default_shift_hrs = {"A": 8, "B": 8, "C": 8, "G": 7}
    shift_hrs = default_shift_hrs.get(str(shift).upper(), 0)

    work_float = _to_float_workhrs(work_hrs_str)
    overtime_val = round(work_float - shift_hrs - 0.60, 2)

    # If OT is negative, return blank
    return "" if overtime_val < 0 else overtime_val

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

def clean_daily_inout13(
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
    print("=" * 80)
    print("[clean_daily_inout13] Starting")
    print(f"[clean_daily_inout13] Input: {input_path}")
    print(f"[clean_daily_inout13] Output: {output_path}")
    print(f"[clean_daily_inout13] Company: {company}")
    print(f"[clean_daily_inout13] Branch: {branch}")
    print(f"[clean_daily_inout13] Custom From Date: {custom_from_date}")
    print(f"[clean_daily_inout13] Custom To Date: {custom_to_date}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Check if .xls file needs conversion
    working_file = input_path
    temp_created = False
    if input_path.lower().endswith(".xls"):
        working_file = convert_xls_to_xlsx(input_path)
        temp_created = True

    df_raw = pd.read_excel(working_file, engine="openpyxl")
    print(f"[clean_daily_inout13] Loaded raw DataFrame shape: {df_raw.shape}")

    required_cols = ["Employee ID", "Attand Date", "Employee Name", "Status", "In Time", "Out Time", "Total Hour"]
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    # 1) Parse file date range from Attand Date column
    file_from_date, file_to_date = parse_file_date_range(df_raw, "Attand Date")

    # 2) Validate and get final date range to process
    filter_from_date, filter_to_date = validate_date_range(
        file_from_date, file_to_date, custom_from_date, custom_to_date
    )
    print(f"[clean_daily_inout13] Processing attendance for: {filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d}")

    records = []
    for _, row in df_raw.iterrows():
        emp_id = str(row.get("Employee ID")).strip() if pd.notna(row.get("Employee ID")) else None
        emp_name = str(row.get("Employee Name")).strip() if pd.notna(row.get("Employee Name")) else None
        att_date_raw = row.get("Attand Date")
        time_in = row.get("In Time")
        time_out = row.get("Out Time")
        work_hrs = str(row.get("Total Hour")).strip() if pd.notna(row.get("Total Hour")) else None
        status_raw = row.get("Status")

        # Parse attendance date in DD/MM/YYYY format first
        parsed_att_date = parse_date_dd_mm_yyyy(att_date_raw)
        if parsed_att_date is None or pd.isna(parsed_att_date):
            print(f"[clean_daily_inout13] WARNING: Could not parse date {att_date_raw} for {emp_id} {emp_name}, skipping row")
            continue

        att_date_str = parsed_att_date.strftime("%Y-%m-%d")

        # Filter by date range
        current_date = parsed_att_date.to_pydatetime() if isinstance(parsed_att_date, pd.Timestamp) else parsed_att_date
        if current_date < filter_from_date or current_date > filter_to_date:
            continue  # Skip dates outside user-selected range

        # Skip blank/empty rows
        if (pd.isna(time_in) or str(time_in).strip() == "") and \
            (pd.isna(time_out) or str(time_out).strip() == "") and \
            (pd.isna(work_hrs) or str(work_hrs).strip() == "") and \
            (pd.isna(status_raw) or str(status_raw).strip() == ""):
            print(f"[clean_daily_inout13] Skipping {emp_id} {emp_name} on {att_date_str} (Empty Row)")
            continue

        # Map Employee ID (Gate Pass No) → Employee
        employee_id = None
        if emp_id:
            try:
                emp_doc = frappe.get_doc("Employee", {"attendance_device_id": emp_id})
                employee_id = emp_doc.name
            except Exception:
                print(f"[clean_daily_inout13] WARNING: Employee not found for GP No {emp_id}")

        # Format In/Out times
        in_time_fmt = format_datetime(parsed_att_date, time_in)
        out_time_fmt = format_datetime(parsed_att_date, time_out)

        # Check if either punch is missing
        in_punch_missing = in_time_fmt is None
        out_punch_missing = out_time_fmt is None
        any_punch_missing = in_punch_missing or out_punch_missing

        # Calculate working hours from In Time and Out Time
        work_hrs_display = ""  # HH.MM format for display
        total_seconds = 0
        if not any_punch_missing:
            # Both punches exist - calculate working hours
            try:
                in_dt = parse_time_to_datetime(att_date_str, in_time_fmt)
                out_dt = parse_time_to_datetime(att_date_str, out_time_fmt)
                if in_dt and out_dt:
                    # Handle overnight shift
                    if out_dt <= in_dt:
                        out_dt += timedelta(days=1)
                    total_seconds = (out_dt - in_dt).total_seconds()
                    work_hrs_display = _seconds_to_decimal_hours(total_seconds)  # HH.MM format
            except Exception:
                pass
        # If any punch missing, work_hrs_display stays blank ("")

        # Convert to hours for status calculation (e.g., 8h 14m = 8.23 hours)
        work_hrs_for_calc = total_seconds / 3600 if total_seconds > 0 else 0

        # Apply working hours threshold logic for status
        if any_punch_missing:
            status = "Absent"
        elif work_hrs_for_calc >= 7.0:
            status = "Present"
        elif work_hrs_for_calc >= 4.5:
            status = "Half Day"
        else:
            status = "Absent"

        print(f"[clean_daily_inout13] {emp_id} {emp_name} on {att_date_str}: Working Hours={work_hrs_display} -> {status}")

        # Detect shift and calculate overtime
        shift = detect_shift(in_time_fmt, out_time_fmt)

        # Overtime calculation - blank if no working hours
        overtime_val = ""
        if work_hrs_for_calc > 0:
            shift_hrs = {"A": 8, "B": 8, "C": 8, "G": 8}.get(str(shift).upper(), 8)
            ot_hours = work_hrs_for_calc - shift_hrs - 1  # 1 hour grace
            if ot_hours > 0:
                # Convert OT to HH.MM format
                ot_hrs = int(ot_hours)
                ot_mins = int((ot_hours - ot_hrs) * 60)
                overtime_val = float(f"{ot_hrs}.{ot_mins:02d}")

        rec = {
            "Attendance Date": att_date_str,
            "Employee": employee_id if employee_id else "",
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time_fmt if in_time_fmt else "",
            "Out Time": out_time_fmt if out_time_fmt else "",
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": work_hrs_display,
            "Shift": shift,
            "Over Time": overtime_val
        }
        records.append(rec)

    df_final = pd.DataFrame.from_records(records)
    print(f"[clean_daily_inout13] Built DataFrame with {len(df_final)} rows (before merge)")

    # Drop records with missing Employee ID
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")
    df_final = df_final[df_final['Employee'] != '']
    print(f"[clean_daily_inout13] After dropping invalid records: {len(df_final)} rows")

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
    print(f"[clean_daily_inout13] Saved output to: {output_path}")
    print(f"[clean_daily_inout13] Processed {len(df_final)} attendance records for date range: {filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d}")

    # Clean up temporary file if created
    if temp_created and os.path.exists(working_file):
        try:
            os.unlink(working_file)
            print(f"[clean_daily_inout13] Removed temporary file: {working_file}")
        except Exception:
            pass

    print("[clean_daily_inout13] Done ✅")

    return df_final
