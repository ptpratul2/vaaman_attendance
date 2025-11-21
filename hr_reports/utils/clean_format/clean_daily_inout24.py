# clean_daily_inout24.py
import os
import pandas as pd
import frappe
from datetime import datetime, timedelta, time

def clean_daily_inout24(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
    print("=" * 80)
    print("[clean_daily_inout24] Starting")
    print(f"[clean_daily_inout24] Input: {input_path}")
    print(f"[clean_daily_inout24] Output: {output_path}")
    print(f"[clean_daily_inout24] Company: {company}")
    print(f"[clean_daily_inout24] Branch: {branch}")
    print("=" * 80)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Load the file
    df_raw = pd.read_excel(input_path, engine="openpyxl")
    print(f"[clean_daily_inout24] Loaded raw DataFrame shape: {df_raw.shape}")

    # Required columns from raw report
    required_cols = ["Name", "Gate Pass", "Date", "Intime", "Outtime", "GROSSHOURS", "Shift"]
    missing = [c for c in required_cols if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    # ------------------------------------------------------
    # Function: format "Extra/Less Hours" → decimal-like str
    # ------------------------------------------------------
    def format_extra_less(v):
        if pd.isna(v):
            return ""

        # Case 1: Excel gave us a datetime.time or Timestamp
        if hasattr(v, "hour") and hasattr(v, "minute"):
            h = v.hour
            m = v.minute
            s = v.second
            # If there are seconds → keep them as .ss, else only hh.mm
            if s:
                return f"{h}.{s:02d}"
            else:
                return f"{h}.{m:02d}"

        # Case 2: It's already a string like -1:-22: or 4:30
        s = str(v).strip()
        if not s:
            return ""

        # Replace last ":" with "."
        if ":" in s:
            parts = s.split(":")
            if len(parts) >= 2:
                return ":".join(parts[:-1]) + "." + parts[-1]

        return s

    def parse_time_to_hours(time_str):
        """Convert time string (HH:MM or HH:MM:SS) to decimal hours"""
        if not time_str or pd.isna(time_str):
            return 0.0
        try:
            time_str = str(time_str).strip()
            parts = time_str.split(":")
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
            seconds = int(parts[2]) if len(parts) > 2 else 0
            return hours + minutes/60 + seconds/3600
        except Exception:
            return 0.0

    def detect_shift_from_checkin(checkin_time_str, att_date):
        """
        Detect shift based on check-in time
        Shift definitions:
        - A shift: 6 AM to 2 PM (check-in 5:30 AM to 2 PM)
        - B shift: 2 PM to 10 PM (check-in 1:30 PM to 10 PM)
        - C shift: 10 PM to 6 AM (check-in 9:30 PM to 6 AM next day)
        - G shift: 9 AM to 5:30 PM (check-in 8:30 AM to 5:30 PM)
        """
        if not checkin_time_str or not att_date:
            return None

        try:
            # Parse check-in time
            checkin_parts = str(checkin_time_str).strip().split(":")
            checkin_hour = int(checkin_parts[0])
            checkin_min = int(checkin_parts[1]) if len(checkin_parts) > 1 else 0
            checkin_time = time(checkin_hour, checkin_min)

            # Parse date
            date_obj = pd.to_datetime(att_date).date()
            checkin_datetime = datetime.combine(date_obj, checkin_time)

            # Shift definitions with 30-minute buffer before start
            # C shift: 10 PM to 6 AM next day (9:30 PM to 5:30 AM next day) - spans midnight
            # Note: C shift ends at 5:30 AM (30 min before A shift starts at 6 AM)
            c_start = datetime.combine(date_obj, time(21, 30))
            c_end = datetime.combine(date_obj + timedelta(days=1), time(5, 30))

            # G shift: 9 AM to 5:30 PM (8:30 AM to 5:30 PM)
            g_start = datetime.combine(date_obj, time(8, 30))
            g_end = datetime.combine(date_obj, time(17, 30))

            # A shift: 6 AM to 2 PM (5:30 AM to 2 PM)
            a_start = datetime.combine(date_obj, time(5, 30))
            a_end = datetime.combine(date_obj, time(14, 0))

            # B shift: 2 PM to 10 PM (1:30 PM to 10 PM)
            b_start = datetime.combine(date_obj, time(13, 30))
            b_end = datetime.combine(date_obj, time(22, 0))

            # Check which shift the check-in time falls into
            # Priority order: C (night shift), G, A, B
            
            # Handle C shift (spans midnight - 9:30 PM to 5:30 AM next day)
            if checkin_datetime >= c_start:
                return "C"
            elif checkin_datetime < datetime.combine(date_obj, time(5, 30)):
                return "C"
            # Check G shift (8:30 AM to 5:30 PM) - overlaps with A and B, check first
            elif g_start <= checkin_datetime <= g_end:
                return "G"
            # Check A shift (5:30 AM to 2 PM, excluding G shift range)
            elif a_start <= checkin_datetime <= a_end:
                return "A"
            # Check B shift (1:30 PM to 10 PM, excluding G shift range)
            elif b_start <= checkin_datetime <= b_end:
                return "B"
            else:
                # Default to A shift if no match (fallback)
                return "A"

        except Exception as e:
            print(f"[clean_daily_inout24] Error detecting shift: {e}")
            return None

    def parse_to_datetime(time_value, att_date):
        """
        Parse time value to datetime object for ERPNext
        Handles both datetime objects and time strings
        """
        if not time_value or not att_date:
            return None

        try:
            # If it's already a datetime object
            if isinstance(time_value, datetime):
                return time_value
            
            # If it's a pandas Timestamp
            if isinstance(time_value, pd.Timestamp):
                return time_value.to_pydatetime()
            
            # Parse date
            date_obj = pd.to_datetime(att_date).date()

            # Parse time string
            time_str = str(time_value).strip()
            # Handle datetime string format (e.g., "2024-01-15 09:30:00")
            if " " in time_str:
                return pd.to_datetime(time_str).to_pydatetime()
            
            # Handle time-only string (HH:MM or HH:MM:SS)
            time_parts = time_str.split(":")
            in_hour = int(time_parts[0])
            in_min = int(time_parts[1]) if len(time_parts) > 1 else 0
            in_sec = int(time_parts[2]) if len(time_parts) > 2 else 0

            # Create datetime object
            return datetime.combine(date_obj, time(in_hour, in_min, in_sec))
        except Exception as e:
            print(f"[clean_daily_inout24] Error parsing datetime: {e}, value: {time_value}")
            return None

    def calculate_working_hours(intime, outtime, att_date):
        """Calculate working hours from intime and outtime (datetime objects)"""
        if not intime or not outtime or not att_date:
            return None, 0.0

        try:
            # Parse to datetime objects
            in_dt = parse_to_datetime(intime, att_date)
            out_dt = parse_to_datetime(outtime, att_date)

            if not in_dt or not out_dt:
                return None, 0.0

            # If outtime is earlier than intime, assume it's next day
            if out_dt < in_dt:
                out_dt += timedelta(days=1)

            # Calculate difference
            diff = out_dt - in_dt
            total_seconds = diff.total_seconds()
            hours = total_seconds / 3600

            # Format as HH:MM:SS
            h = int(hours)
            m = int((hours - h) * 60)
            s = int(((hours - h) * 60 - m) * 60)
            work_hrs_str = f"{h:02d}:{m:02d}:{s:02d}"

            return work_hrs_str, hours
        except Exception as e:
            print(f"[clean_daily_inout24] Error calculating hours: {e}")
            return None, 0.0

    records = []

    for _, row in df_raw.iterrows():
        gate_pass = str(row.get("Gate Pass")).strip() if pd.notna(row.get("Gate Pass")) else None
        emp_name = str(row.get("Name")).strip() if pd.notna(row.get("Name")) else None
        att_date = pd.to_datetime(row.get("Date")).strftime("%Y-%m-%d") if pd.notna(row.get("Date")) else None
        intime_raw = row.get("Intime") if pd.notna(row.get("Intime")) else None
        outtime_raw = row.get("Outtime") if pd.notna(row.get("Outtime")) else None
        gross_hours = str(row.get("GROSSHOURS")).strip() if pd.notna(row.get("GROSSHOURS")) else None
        shift = str(row.get("Shift")).strip() if pd.notna(row.get("Shift")) and str(row.get("Shift")).strip() else None
        over_time = format_extra_less(row.get("Extra/Less Hours"))

        # ------------------------
        # Parse intime/outtime to datetime objects for ERPNext
        # ------------------------
        intime_dt = parse_to_datetime(intime_raw, att_date) if intime_raw else None
        outtime_dt = parse_to_datetime(outtime_raw, att_date) if outtime_raw else None

        # Format datetime for ERPNext (YYYY-MM-DD HH:MM:SS)
        intime_str = intime_dt.strftime("%Y-%m-%d %H:%M:%S") if intime_dt else ""
        outtime_str = outtime_dt.strftime("%Y-%m-%d %H:%M:%S") if outtime_dt else ""

        # ------------------------
        # Auto-detect shift if blank
        # ------------------------
        if not shift and intime_dt:
            detected_shift = detect_shift_from_checkin(intime_raw, att_date)
            if detected_shift:
                shift = detected_shift
                print(f"[clean_daily_inout24] Auto-detected shift '{shift}' for check-in {intime_str}")

        # ------------------------
        # Map Gate Pass → Employee ID
        # ------------------------
        employee_id = None
        try:
            emp_doc = frappe.get_doc("Employee", {"attendance_device_id": gate_pass})
            employee_id = emp_doc.name
        except Exception:
            print(f"[clean_daily_inout24] WARNING: Employee not found for Gate Pass {gate_pass}")

        # ------------------------
        # Calculate Working Hours and Determine Status
        # ------------------------
        status = "Absent"
        working_hours_str = ""  # Will be calculated from Intime/Outtime

        # If both Intime and Outtime are present, calculate working hours
        if intime_dt and outtime_dt:
            calc_work_hrs, total_hours = calculate_working_hours(intime_dt, outtime_dt, att_date)

            if calc_work_hrs:
                working_hours_str = calc_work_hrs

                # Determine status based on working hours
                if total_hours >= 7:
                    status = "Present"
                elif total_hours >= 4.5:  # 4:30 = 4.5 hours
                    status = "Half Day"
                else:
                    status = "Absent"
        # If Intime or Outtime is missing, mark as Absent with no working hours
        else:
            status = "Absent"
            working_hours_str = ""

        rec = {
            "Attendance Date": att_date,
            "Employee": employee_id if employee_id else "",
            "Employee Name": emp_name,
            "Status": status,
            "In Time": intime_str,  # Datetime format for ERPNext
            "Out Time": outtime_str,  # Datetime format for ERPNext
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": working_hours_str,
            "Shift": shift if shift else "",
            "Over Time": over_time
        }
        records.append(rec)

    df_final = pd.DataFrame.from_records(records)

    # Drop rows without employee/date
    df_final = df_final.dropna(subset=["Attendance Date", "Employee"], how="any")
    print(f"[clean_daily_inout24] Built final DataFrame with {len(df_final)} rows")

    if df_final.empty:
        raise ValueError("No attendance records parsed from Daily In-Out report.")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout24] Saved output to: {output_path}")
    print("[clean_daily_inout24] Done ✅")

    return df_final
