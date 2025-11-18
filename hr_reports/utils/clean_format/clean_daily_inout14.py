import os
import pandas as pd
import frappe
from datetime import datetime, timedelta

def format_datetime(date_val, time_val):
    """
    Combine Date + Timedelta/Time into dd-mm-YYYY hh:mm:ss AM/PM format.
    Fallback: use 09:00 AM for In, 05:00 PM for Out if missing.
    """
    if pd.isna(date_val):
        return None

    # Normalize date
    if not isinstance(date_val, (datetime, pd.Timestamp)):
        date_val = pd.to_datetime(date_val, errors="coerce")
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

        # Convert to both formats: string for display, float for Frappe
        work_hrs_str = _seconds_to_workhrs(total_seconds)
        work_hrs_decimal = _seconds_to_decimal_hours(total_seconds)  # Decimal hours for Frappe
        overtime = _calculate_overtime_from_seconds(total_seconds)

        status = "Present" if total_seconds and total_seconds > 0 else group.iloc[0]['Status']

        merged_row = {
            'Employee': emp,
            'Attendance Date': date,
            'Employee Name': group.iloc[0]['Employee Name'],
            'Status': status,
            'In Time': _format_output_datetime(earliest_in) or _first_non_empty(group['In Time']),
            'Out Time': _format_output_datetime(latest_out) or _first_non_empty(group['Out Time']),
            'Company': group.iloc[0]['Company'],
            'Branch': group.iloc[0]['Branch'],
            'Working Hours': work_hrs_decimal,  # Use decimal hours for Frappe
            'Shift': shift,
            'Over Time': overtime
        }
        merged_rows.append(merged_row)
        print(f"[clean_daily_inout14] Merged {len(group)} punches for {emp} on {date} - Total working hours: {work_hrs_decimal} hours ({work_hrs_str}) (from {len(valid_pairs)} valid in/out pairs)")

    merged_df = pd.DataFrame(merged_rows)
    print(f"[clean_daily_inout14] Merge: Completed. Final count: {len(merged_df)} records")

    return merged_df


def clean_daily_inout14(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout14] Starting")
    print(f"[clean_daily_inout14] Input: {input_path}")
    print(f"[clean_daily_inout14] Output: {output_path}")
    print(f"[clean_daily_inout14] Company: {company}")
    print(f"[clean_daily_inout14] Branch: {branch}")
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

    records = []
    failed_gp_count = 0
    for idx, row in df_raw.iterrows():
        gp_no = str(row.get("GP No")).strip() if pd.notna(row.get("GP No")) else None
        emp_name = str(row.get("Name")).strip() if pd.notna(row.get("Name")) else None
        att_date = row.get("Date In")
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

        # Status logic
        status = "Absent"
        if work_hrs and work_hrs not in ["", "0:00", "00:00"]:
            status = "Present"

        # Format In/Out time
        in_time_fmt = format_datetime(att_date, time_in) or format_datetime(att_date, "09:00:00")
        out_time_fmt = format_datetime(att_date, time_out) or format_datetime(att_date, "17:00:00")

        # Calculate working hours from in/out times if available, otherwise use Excel value
        work_hrs_decimal = 0.0
        if time_in and time_out and pd.notna(time_in) and pd.notna(time_out):
            try:
                # Try to calculate from in/out times
                in_dt = parse_time_to_datetime(
                    pd.to_datetime(att_date).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
                    in_time_fmt
                )
                out_dt = parse_time_to_datetime(
                    pd.to_datetime(att_date).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
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
        else:
            # Use Excel value converted to decimal
            work_hrs_decimal = _to_float_workhrs(work_hrs)

        # Overtime calculation (using decimal hours)
        overtime_val = _calculate_overtime_from_seconds(work_hrs_decimal * 3600) if work_hrs_decimal > 0 else ""

        rec = {
            "Attendance Date": pd.to_datetime(att_date).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
            "Employee": employee_id if employee_id else "",
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time_fmt,
            "Out Time": out_time_fmt,
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": work_hrs_decimal,  # Use decimal hours for Frappe
            "Shift": shift if shift else "",
            "Over Time": overtime_val
        }
        records.append(rec)

    df_final = pd.DataFrame.from_records(records)

    print(f"[clean_daily_inout14] Built DataFrame with {len(df_final)} rows (before dropping invalid)")
    if failed_gp_count > 0:
        print(f"[clean_daily_inout14] Total GP Numbers not found: {failed_gp_count}")

    # Debug: Check Employee column before drop
    empty_employees = df_final[df_final['Employee'].isna() | (df_final['Employee'] == '')]
    if not empty_employees.empty:
        print(f"[clean_daily_inout14] WARNING: Found {len(empty_employees)} rows with missing Employee ID")

    # Drop invalid
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")
    df_final = df_final[df_final['Employee'] != '']  # Also remove empty strings
    print(f"[clean_daily_inout14] After dropping invalid: {len(df_final)} rows (before merging)")

    # Merge overlapping attendances
    if not df_final.empty:
        print(f"[clean_daily_inout14] Starting merge process...")
        try:
            df_final = merge_overlapping_attendances(df_final)
            print(f"[clean_daily_inout14] After merging overlaps: {len(df_final)} rows")
        except Exception as e:
            print(f"[clean_daily_inout14] ERROR in merge function: {str(e)}")
            import traceback
            traceback.print_exc()
            print(f"[clean_daily_inout14] Continuing without merge...")

    if df_final.empty:
        raise ValueError("No attendance records parsed from Daily In-Out report.")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout14] Saved output to: {output_path}")
    print("[clean_daily_inout14] Done ✅")

    return df_final
