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
    """Convert 'HH:MM:SS' → float hours e.g. '08:53:09' → 8.53"""
    if not time_str or str(time_str).lower() in ["nan", "none"]:
        return 0.0
    try:
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
    Merge attendance records for same employee on same date.
    Simple approach: Find duplicates, merge only those, keep rest as-is.
    """
    if df.empty:
        return df

    print(f"[clean_daily_inout14] Merge: Processing {len(df)} records...")

    # Step 1: Find duplicates - much faster!
    df['_is_duplicate'] = df.duplicated(subset=['Employee', 'Attendance Date'], keep=False)

    duplicates = df[df['_is_duplicate'] == True].copy()
    non_duplicates = df[df['_is_duplicate'] == False].copy()

    print(f"[clean_daily_inout14] Merge: Found {len(duplicates)} duplicate records, {len(non_duplicates)} unique records")

    # If no duplicates, return as-is
    if duplicates.empty:
        print(f"[clean_daily_inout14] Merge: No duplicates found, skipping merge")
        return non_duplicates.drop(columns=['_is_duplicate'])

    # Step 2: Only process duplicates
    print(f"[clean_daily_inout14] Merge: Processing only the {len(duplicates)} duplicate records...")

    # Parse datetimes only for duplicates
    duplicates['_in_dt'] = duplicates.apply(lambda r: parse_time_to_datetime(r['Attendance Date'], r['In Time']), axis=1)
    duplicates['_out_dt'] = duplicates.apply(lambda r: parse_time_to_datetime(r['Attendance Date'], r['Out Time']), axis=1)
    duplicates['_work_float'] = duplicates['Working Hours'].apply(_to_float_workhrs)

    # Step 3: Merge duplicates
    merged_list = []

    for (emp, date), group in duplicates.groupby(['Employee', 'Attendance Date']):
        # Get earliest In Time
        earliest_in = group.iloc[0]['In Time']
        if group['_in_dt'].notna().any():
            earliest_idx = group['_in_dt'].idxmin()
            earliest_in = group.loc[earliest_idx, 'In Time']

        # Get latest Out Time
        latest_out = group.iloc[0]['Out Time']
        if group['_out_dt'].notna().any():
            latest_idx = group['_out_dt'].idxmax()
            latest_out = group.loc[latest_idx, 'Out Time']

        # Sum working hours
        total_work_float = group['_work_float'].sum()
        hours = int(total_work_float)
        minutes = int((total_work_float - hours) * 60)
        seconds = int(((total_work_float - hours) * 60 - minutes) * 60)
        work_hrs_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        # Calculate overtime
        shift = group.iloc[0]['Shift']
        ot = _calculate_overtime(work_hrs_str, shift)

        # Create merged record
        merged = {
            'Employee': emp,
            'Attendance Date': date,
            'Employee Name': group.iloc[0]['Employee Name'],
            'Status': group.iloc[0]['Status'],
            'In Time': earliest_in,
            'Out Time': latest_out,
            'Company': group.iloc[0]['Company'],
            'Branch': group.iloc[0]['Branch'],
            'Working Hours': work_hrs_str,
            'Shift': shift,
            'Over Time': ot
        }
        merged_list.append(merged)
        print(f"[clean_daily_inout14] Merged {len(group)} records for {emp} on {date}")

    # Step 4: Combine non-duplicates with merged duplicates
    merged_df = pd.DataFrame(merged_list)
    non_duplicates = non_duplicates.drop(columns=['_is_duplicate'])

    result = pd.concat([non_duplicates, merged_df], ignore_index=True)

    print(f"[clean_daily_inout14] Merge: Completed. Final count: {len(result)} records")

    return result


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

        # Overtime calculation
        overtime_val = _calculate_overtime(work_hrs, shift)

        rec = {
            "Attendance Date": pd.to_datetime(att_date).strftime("%Y-%m-%d") if pd.notna(att_date) else "",
            "Employee": employee_id if employee_id else "",
            "Employee Name": emp_name,
            "Status": status,
            "In Time": in_time_fmt,
            "Out Time": out_time_fmt,
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": work_hrs,
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
