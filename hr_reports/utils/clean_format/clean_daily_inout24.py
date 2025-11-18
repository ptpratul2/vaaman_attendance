# clean_daily_inout24.py
import os
import pandas as pd
import frappe

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

    # Case 2: It’s already a string like -1:-22: or 4:30
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

    def calculate_working_hours(intime, outtime, att_date):
        """Calculate working hours from intime and outtime"""
        if not intime or not outtime or not att_date:
            return None, 0.0

        try:
            from datetime import datetime, timedelta

            # Parse date
            date_obj = pd.to_datetime(att_date).date()

            # Parse intime
            intime_str = str(intime).strip()
            in_parts = intime_str.split(":")
            in_hour = int(in_parts[0])
            in_min = int(in_parts[1]) if len(in_parts) > 1 else 0
            in_sec = int(in_parts[2]) if len(in_parts) > 2 else 0

            # Parse outtime
            outtime_str = str(outtime).strip()
            out_parts = outtime_str.split(":")
            out_hour = int(out_parts[0])
            out_min = int(out_parts[1]) if len(out_parts) > 1 else 0
            out_sec = int(out_parts[2]) if len(out_parts) > 2 else 0

            # Create datetime objects
            in_dt = datetime.combine(date_obj, datetime.min.time().replace(hour=in_hour, minute=in_min, second=in_sec))
            out_dt = datetime.combine(date_obj, datetime.min.time().replace(hour=out_hour, minute=out_min, second=out_sec))

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
        intime = str(row.get("Intime")).strip() if pd.notna(row.get("Intime")) else None
        outtime = str(row.get("Outtime")).strip() if pd.notna(row.get("Outtime")) else None
        gross_hours = str(row.get("GROSSHOURS")).strip() if pd.notna(row.get("GROSSHOURS")) else None
        shift = str(row.get("Shift")).strip() if pd.notna(row.get("Shift")) else None
        over_time = format_extra_less(row.get("Extra/Less Hours"))

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
        if intime and outtime:
            calc_work_hrs, total_hours = calculate_working_hours(intime, outtime, att_date)

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
            "In Time": intime,
            "Out Time": outtime,
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
