import os
import pandas as pd
import frappe
from datetime import datetime, timedelta
from typing import Optional, Tuple


# =========================
#  Date Range Helpers
# =========================
def parse_file_date_range(df: pd.DataFrame, date_column: str = "Date In") -> Tuple[datetime, datetime]:
    """
    Extract min and max dates from the Date In column.
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


def format_datetime(date_val, time_val):
    """
    Combine Date + Timedelta/Time into dd-mm-YYYY hh:mm:ss AM/PM format.
    Fallback: use 09:00 AM for In, 05:00 PM for Out if missing.
    """
    if pd.isna(date_val):
        return None

    # Normalize date
    if not isinstance(date_val, (datetime, pd.Timestamp)):
        date_val = pd.to_datetime(date_val, dayfirst=True, errors="coerce")
    if pd.isna(date_val):
        return None

    # If time is timedelta (from Excel)
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

    # AM/PM adjustment
    suffix = "AM"
    if hours >= 12:
        suffix = "PM"
        if hours > 12:
            hours -= 12
    elif hours == 0:
        hours = 12  # midnight = 12 AM

    return date_val.strftime("%d-%m-%Y") + f" {hours:02d}:{minutes:02d}:{seconds:02d} {suffix}"


def _to_float_workhrs(time_str):
    """Convert 'HH:MM:SS' → float hours e.g. '08:53:09' → 8.89"""
    if not time_str or str(time_str).lower() in ["nan", "none"]:
        return 0.0
    try:
        # Handle if already a number
        if isinstance(time_str, (int, float)):
            return float(time_str)
        
        parts = str(time_str).split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return round(h + m/60 + s/3600, 2)  # keep 2 decimals
    except Exception:
        return 0.0


def _calculate_overtime(work_hrs_str, shift):
    """
    Overtime calculation as per logic:
    - All shifts considered as 9 hours
    - OT = Working Hours - 9
    - If OT is negative or less than 1 hour, return empty string (blank cell)
    """
    work_float = _to_float_workhrs(work_hrs_str)
    shift_hrs = 9  # All shifts are 9 hours
    overtime = round(work_float - shift_hrs, 2)

    # If OT is negative or less than 1 hour, return blank
    if overtime < 1:
        return ""

    return overtime


def _seconds_to_workhrs(total_seconds: float) -> str:
    """Convert seconds → HH:MM:SS"""
    if not total_seconds or total_seconds <= 0:
        return "00:00:00"
    total_seconds = int(total_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _seconds_to_decimal_hours(total_seconds: float) -> float:
    """Convert seconds → decimal hours (float) for Frappe working_hours field"""
    if not total_seconds or total_seconds <= 0:
        return 0.0
    # Round to 2 decimal places (e.g., 8.5 for 8 hours 30 minutes)
    return round(total_seconds / 3600, 2)


def _calculate_overtime_from_seconds(total_seconds: float, shift_hours: int = 9):
    """Overtime calculation based on total seconds worked."""
    if not total_seconds or total_seconds <= 0:
        return ""
    work_float = round(total_seconds / 3600, 2)
    overtime = round(work_float - shift_hours, 2)
    if overtime < 1:
        return ""
    return overtime


def _format_output_datetime(dt_obj: datetime) -> str:
    """Return dd-mm-YYYY hh:mm:ss AM/PM format from datetime"""
    if not dt_obj:
        return ""
    return dt_obj.strftime("%d-%m-%Y %I:%M:%S %p")


def _merge_intervals(intervals):
    """Merge overlapping/touching datetime intervals."""
    intervals = [(start, end) for start, end in intervals if start and end]
    if not intervals:
        return []

    intervals.sort(key=lambda x: x[0])
    merged = [list(intervals[0])]

    for start, end in intervals[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1][1] = max(last_end, end)
        else:
            merged.append([start, end])
    return merged


def _first_non_empty(series: pd.Series) -> str:
    """Return first non-empty/non-null string value from a Series."""
    for value in series:
        if pd.isna(value):
            continue
        value_str = str(value).strip()
        if value_str:
            return value_str
    return ""


def parse_time_to_datetime(date_str, time_str):
    """Parse date and time string to datetime object"""
    try:
        # Parse date: YYYY-MM-DD
        date_obj = pd.to_datetime(date_str).date()

        # Parse time: dd-mm-YYYY hh:mm:ss AM/PM
        if pd.isna(time_str) or not time_str:
            return None

        time_parts = str(time_str).split()
        if len(time_parts) < 2:
            return None

        # Get time part and AM/PM
        time_part = time_parts[-2]  # hh:mm:ss
        am_pm = time_parts[-1]       # AM/PM

        # Parse time
        h, m, s = map(int, time_part.split(':'))

        # Convert to 24-hour format
        if am_pm.upper() == 'PM' and h != 12:
            h += 12
        elif am_pm.upper() == 'AM' and h == 12:
            h = 0

        return datetime.combine(date_obj, datetime.min.time().replace(hour=h, minute=m, second=s))
    except Exception:
        return None


def times_overlap(start1, end1, start2, end2):
    """Check if two time ranges overlap"""
    if not all([start1, end1, start2, end2]):
        return False
    return start1 < end2 and start2 < end1


def merge_overlapping_attendances(df):
    """
    Merge all punches for each Employee+Attendance Date pair.
    - Assign one shift per day
    - Calculate working hours as sum of actual time between each in/out pair
    - Support overnight shifts
    - Calculate OT after 9 hours
    """
    if df.empty:
        return df

    working_df = df.copy()

    print(f"[clean_daily_inout14] Merge: Processing {len(working_df)} records...")

    working_df['_in_dt'] = working_df.apply(
        lambda r: parse_time_to_datetime(r['Attendance Date'], r['In Time']),
        axis=1
    )
    working_df['_out_dt'] = working_df.apply(
        lambda r: parse_time_to_datetime(r['Attendance Date'], r['Out Time']),
        axis=1
    )

    # Overnight support: if Out <= In, push Out to next day
    overnight_mask = (
        working_df['_in_dt'].notna() &
        working_df['_out_dt'].notna() &
        (working_df['_out_dt'] <= working_df['_in_dt'])
    )
    working_df.loc[overnight_mask, '_out_dt'] += timedelta(days=1)

    merged_rows = []

    for (emp, date), group in working_df.groupby(['Employee', 'Attendance Date']):
        if not emp or not date:
            continue

        # Calculate working hours from actual time between each in/out pair
        # Sum all individual punch durations
        total_seconds = 0.0
        valid_pairs = []
        
        for idx, row in group.iterrows():
            in_dt = row['_in_dt']
            out_dt = row['_out_dt']
            
            # Only count pairs where both in and out are valid
            if pd.notna(in_dt) and pd.notna(out_dt):
                if out_dt > in_dt:  # Ensure out is after in
                    duration = (out_dt - in_dt).total_seconds()
                    if duration > 0:
                        total_seconds += duration
                        valid_pairs.append((in_dt, out_dt))
        
        # Get earliest in and latest out for display
        earliest_in_series = group['_in_dt'].dropna()
        latest_out_series = group['_out_dt'].dropna()

        earliest_in = earliest_in_series.min() if not earliest_in_series.empty else None
        latest_out = latest_out_series.max() if not latest_out_series.empty else None

        if earliest_in is not None and pd.isna(earliest_in):
            earliest_in = None
        if latest_out is not None and pd.isna(latest_out):
            latest_out = None

        # If no valid pairs but we have earliest/latest, use that as fallback
        if total_seconds <= 0 and earliest_in and latest_out:
            if latest_out > earliest_in:
                total_seconds = (latest_out - earliest_in).total_seconds()

        shift_series = group['Shift'].replace("", pd.NA).dropna()
        shift = shift_series.iloc[0] if not shift_series.empty else ""

        # Check if any punch is missing after merge
        in_punch_missing = earliest_in is None
        out_punch_missing = latest_out is None
        any_punch_missing = in_punch_missing or out_punch_missing

        # Working hours - blank if any punch missing
        if any_punch_missing:
            work_hrs_decimal = ""
            work_hrs_str = ""
            overtime = ""
            status = "Absent"
        else:
            # Both punches exist - calculate normally
            work_hrs_str = _seconds_to_workhrs(total_seconds)
            work_hrs_decimal = _seconds_to_decimal_hours(total_seconds)
            overtime = _calculate_overtime_from_seconds(total_seconds)

            # Status logic based on working hours thresholds
            status = "Absent"
            if work_hrs_decimal >= 7.0:
                status = "Present"
            elif work_hrs_decimal >= 4.5:
                status = "Half Day"

        # Format In/Out times - blank if missing
        in_time_display = _format_output_datetime(earliest_in) if earliest_in else ""
        out_time_display = _format_output_datetime(latest_out) if latest_out else ""

        merged_row = {
            'Employee': emp,
            'Attendance Date': date,
            'Employee Name': group.iloc[0]['Employee Name'],
            'Status': status,
            'In Time': in_time_display,
            'Out Time': out_time_display,
            'Company': group.iloc[0]['Company'],
            'Branch': group.iloc[0]['Branch'],
            'Working Hours': work_hrs_decimal,
            'Shift': shift,
            'Over Time': overtime
        }
        merged_rows.append(merged_row)

        if any_punch_missing:
            print(f"[clean_daily_inout14] Merged {len(group)} punches for {emp} on {date} - MISSING PUNCH (In: {'Yes' if not in_punch_missing else 'No'}, Out: {'Yes' if not out_punch_missing else 'No'}) → Status: Absent")
        else:
            print(f"[clean_daily_inout14] Merged {len(group)} punches for {emp} on {date} - Total working hours: {work_hrs_decimal} hours ({work_hrs_str}) (from {len(valid_pairs)} valid in/out pairs)")

    merged_df = pd.DataFrame(merged_rows)
    print(f"[clean_daily_inout14] Merge: Completed. Final count: {len(merged_df)} records")

    return merged_df


def clean_daily_inout14(
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
    print("[clean_daily_inout14] Starting")
    print(f"[clean_daily_inout14] Input: {input_path}")
    print(f"[clean_daily_inout14] Output: {output_path}")
    print(f"[clean_daily_inout14] Company: {company}")
    print(f"[clean_daily_inout14] Branch: {branch}")
    print(f"[clean_daily_inout14] Custom From Date: {custom_from_date}")
    print(f"[clean_daily_inout14] Custom To Date: {custom_to_date}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Load file - skip header rows (row 0 is title, row 1 is actual headers)
    # Try reading with header on different rows to find the correct one
    df_raw = None
    print("[clean_daily_inout14] Searching for header row...")
    for skip_rows in range(10):  # Try first 10 rows (0-9)
        try:
            temp_df = pd.read_excel(input_path, engine="openpyxl", header=skip_rows)
            # Check if this row has the columns we need
            print(f"  Row {skip_rows}: {list(temp_df.columns[:3])}...")  # Show first 3 columns
            if "GP No" in temp_df.columns:
                df_raw = temp_df
                print(f"[clean_daily_inout14] ✓ Found header row at position {skip_rows}")
                break
        except Exception as e:
            print(f"  Row {skip_rows}: Error - {str(e)[:50]}")
            continue

    if df_raw is None:
        print("[clean_daily_inout14] WARNING: Could not find header row with 'GP No', using default")
        df_raw = pd.read_excel(input_path, engine="openpyxl")

    print(f"[clean_daily_inout14] Loaded raw DataFrame shape: {df_raw.shape}")

    # DEBUG: Print all detected columns
    print("\n" + "=" * 80)
    print("[DEBUG] DETECTED COLUMNS FROM EXCEL:")
    print("=" * 80)
    for idx, col in enumerate(df_raw.columns, 1):
        print(f"{idx}. '{col}' (type: {type(col).__name__}, repr: {repr(col)})")
    print("=" * 80 + "\n")

    # Required cols
    required_cols = ["GP No", "Name", "Date In", "Time In", "Time Out", "Working Hours", "Came In Shift"]
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        print("\n[DEBUG] REQUIRED COLUMNS:")
        for idx, col in enumerate(required_cols, 1):
            print(f"{idx}. '{col}' (repr: {repr(col)})")
        print("\n[DEBUG] MISSING COLUMNS:")
        for col in missing:
            print(f"  - '{col}'")
        raise ValueError(f"Missing required columns in input: {missing}")

    # 1) Parse file date range from Date In column
    file_from_date, file_to_date = parse_file_date_range(df_raw, "Date In")

    # 2) Validate and get final date range to process
    filter_from_date, filter_to_date = validate_date_range(
        file_from_date, file_to_date, custom_from_date, custom_to_date
    )
    print(f"[clean_daily_inout14] Processing attendance for: {filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d}")

    records = []
    failed_gp_count = 0
    for idx, row in df_raw.iterrows():
        gp_no = str(row.get("GP No")).strip() if pd.notna(row.get("GP No")) else None
        emp_name = str(row.get("Name")).strip() if pd.notna(row.get("Name")) else None
        att_date = row.get("Date In")

        # DEBUG: Print first 5 raw dates from Excel
        if idx < 5:
            print(f"[DEBUG] Row {idx}: Raw 'Date In' from Excel: {repr(att_date)} (type: {type(att_date).__name__})")

        time_in = row.get("Time In")
        time_out = row.get("Time Out")
        work_hrs = str(row.get("Working Hours")).strip() if pd.notna(row.get("Working Hours")) else None
        shift = str(row.get("Came In Shift")).strip() if pd.notna(row.get("Came In Shift")) else None

        # Convert "O" shift to "A" shift
        if shift and shift.upper() == "O":
            shift = "A"

        # Map GP No → Employee
        employee_id = None
        if gp_no:
            try:
                emp_doc = frappe.get_doc("Employee", {"attendance_device_id": gp_no})
                employee_id = emp_doc.name
            except Exception as e:
                failed_gp_count += 1
                if failed_gp_count <= 5:  # Only print first 5
                    print(f"[clean_daily_inout14] WARNING: Employee not found for GP No '{gp_no}' (Name: {emp_name}) - Error: {str(e)[:100]}")

        # Format In/Out time - keep blank if missing (no fake defaults)
        in_time_fmt = format_datetime(att_date, time_in)  # Returns None if missing
        out_time_fmt = format_datetime(att_date, time_out)  # Returns None if missing

        # Check if either punch is missing
        in_punch_missing = in_time_fmt is None
        out_punch_missing = out_time_fmt is None
        any_punch_missing = in_punch_missing or out_punch_missing

        # Calculate working hours - blank if any punch is missing
        work_hrs_decimal = ""  # Default to blank
        if not any_punch_missing:
            # Both punches exist - calculate working hours
            try:
                in_dt = parse_time_to_datetime(
                    pd.to_datetime(att_date, dayfirst=True).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
                    in_time_fmt
                )
                out_dt = parse_time_to_datetime(
                    pd.to_datetime(att_date, dayfirst=True).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
                    out_time_fmt
                )
                if in_dt and out_dt:
                    if out_dt <= in_dt:
                        out_dt += timedelta(days=1)  # Handle overnight
                    total_seconds = (out_dt - in_dt).total_seconds()
                    work_hrs_decimal = _seconds_to_decimal_hours(total_seconds)
            except Exception:
                # Fallback to Excel value
                work_hrs_decimal = _to_float_workhrs(work_hrs)
        # If any punch missing, work_hrs_decimal stays blank ("")

        # Status logic
        if any_punch_missing:
            # Missing punch = Absent
            status = "Absent"
        else:
            # Both punches exist - calculate status from working hours
            status = "Absent"
            if isinstance(work_hrs_decimal, (int, float)) and work_hrs_decimal >= 7.0:
                status = "Present"
            elif isinstance(work_hrs_decimal, (int, float)) and work_hrs_decimal >= 4.5:
                status = "Half Day"

        # Overtime calculation (using decimal hours) - blank if working hours is blank
        overtime_val = ""
        if isinstance(work_hrs_decimal, (int, float)) and work_hrs_decimal > 0:
            overtime_val = _calculate_overtime_from_seconds(work_hrs_decimal * 3600)

        # Parse attendance date
        parsed_date = pd.to_datetime(att_date, dayfirst=True).strftime("%Y-%m-%d") if pd.notna(att_date) else ""

        # DEBUG: Print first 5 parsed dates
        if idx < 5:
            print(f"[DEBUG] Row {idx}: Parsed 'Attendance Date': {parsed_date}")

        # Filter by date range
        if parsed_date:
            try:
                current_date = datetime.strptime(parsed_date, "%Y-%m-%d")
                if current_date < filter_from_date or current_date > filter_to_date:
                    continue  # Skip dates outside user-selected range
            except Exception:
                pass  # If date parsing fails, include the record

        rec = {
            "Attendance Date": parsed_date,
            "Employee": employee_id if employee_id else "",
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time_fmt if in_time_fmt else "",  # Blank if missing
            "Out Time": out_time_fmt if out_time_fmt else "",  # Blank if missing
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": work_hrs_decimal,  # Blank if punch missing, decimal hours otherwise
            "Shift": shift if shift else "",
            "Over Time": overtime_val
        }
        records.append(rec)

    df_final = pd.DataFrame.from_records(records)

    print(f"[clean_daily_inout14] Built DataFrame with {len(df_final)} rows (before dropping invalid)")
    if failed_gp_count > 0:
        print(f"[clean_daily_inout14] Total GP Numbers not found: {failed_gp_count}")

    # Check for records with missing Employee ID (GP No not found in master)
    # These will cause Data Import errors - which is correct behavior
    empty_employees = df_final[df_final['Employee'].isna() | (df_final['Employee'] == '')]
    if not empty_employees.empty:
        print(f"[clean_daily_inout14] INFO: {len(empty_employees)} records have missing Employee ID (GP No not in Employee master)")
        print(f"[clean_daily_inout14] INFO: These will show as errors in Data Import - please add missing GP Numbers to Employee master")
        # Log first 10 missing GP Numbers for reference
        for idx, row in empty_employees.head(10).iterrows():
            print(f"  - GP No not found: {row.get('Employee Name', 'Unknown')} on {row.get('Attendance Date', 'Unknown')}")
        if len(empty_employees) > 10:
            print(f"  ... and {len(empty_employees) - 10} more")

    # Only drop records with missing Attendance Date (invalid data)
    # Keep records with missing Employee - Data Import will show the error
    df_final = df_final.dropna(subset=["Attendance Date"], how="any")
    print(f"[clean_daily_inout14] After dropping invalid dates: {len(df_final)} rows (before merging)")

    # Merge overlapping attendances
    # Only merge records WITH Employee ID - records without Employee are kept as-is
    if not df_final.empty:
        print(f"[clean_daily_inout14] Starting merge process...")

        # Separate records with and without Employee
        has_employee = df_final[df_final['Employee'].notna() & (df_final['Employee'] != '')]
        no_employee = df_final[df_final['Employee'].isna() | (df_final['Employee'] == '')]

        print(f"[clean_daily_inout14] Records with Employee: {len(has_employee)}, Records without Employee: {len(no_employee)}")

        try:
            # Only merge records that have Employee ID
            if not has_employee.empty:
                merged_with_emp = merge_overlapping_attendances(has_employee)
                print(f"[clean_daily_inout14] After merging (with Employee): {len(merged_with_emp)} rows")
            else:
                merged_with_emp = has_employee

            # Combine: merged records + unmerged records (no Employee)
            df_final = pd.concat([merged_with_emp, no_employee], ignore_index=True)
            print(f"[clean_daily_inout14] Total after merge: {len(df_final)} rows")

        except Exception as e:
            print(f"[clean_daily_inout14] ERROR in merge function: {str(e)}")
            import traceback
            traceback.print_exc()
            print(f"[clean_daily_inout14] Continuing without merge...")

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
    print(f"[clean_daily_inout14] Saved output to: {output_path}")
    print(f"[clean_daily_inout14] Processed {len(df_final)} attendance records for date range: {filter_from_date:%Y-%m-%d} to {filter_to_date:%Y-%m-%d}")
    print("[clean_daily_inout14] Done ✅")

    return df_final
