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

    records = []

    for _, row in df_raw.iterrows():
        gate_pass = str(row.get("Gate Pass")).strip() if pd.notna(row.get("Gate Pass")) else None
        emp_name = str(row.get("Name")).strip() if pd.notna(row.get("Name")) else None
        att_date = pd.to_datetime(row.get("Date")).strftime("%Y-%m-%d") if pd.notna(row.get("Date")) else None
        intime = str(row.get("Intime")).strip() if pd.notna(row.get("Intime")) else None
        outtime = str(row.get("Outtime")).strip() if pd.notna(row.get("Outtime")) else None
        gross_hours = str(row.get("GROSSHOURS")).strip() if pd.notna(row.get("GROSSHOURS")) else None
        shift = str(row.get("Shift")).strip() if pd.notna(row.get("Shift")) else None

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
        # Determine Status
        # ------------------------
        status = "Absent"
        if gross_hours and gross_hours.strip() not in ["", "0:00", "00:00"]:
            status = "Present"

        rec = {
            "Attendance Date": att_date,
            "Employee": employee_id if employee_id else "",
            "Employee Name": emp_name,
            "Status": status,
            "In Time": intime,
            "Out Time": outtime,
            "Company": company if company else "",
            "Branch": branch if branch else "",
            "Working Hours": gross_hours,
            "Shift": shift if shift else ""
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
