# clean_crystal_excel.py
import os
import re
import tempfile
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import frappe

import pandas as pd
import xlrd
from openpyxl import Workbook

# -------------------------
# .xls -> .xlsx conversion
# -------------------------
def convert_xls_to_xlsx(xls_path: str) -> str:
   print(f"[convert_xls_to_xlsx] Converting .xls to .xlsx: {xls_path}")
   book = xlrd.open_workbook(xls_path, formatting_info=False)
   sheet = book.sheet_by_index(0)
   print(f"[convert_xls_to_xlsx] Original .xls dimensions: {sheet.nrows} rows x {sheet.ncols} cols")

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
   print(f"[convert_xls_to_xlsx] Saved temporary .xlsx file: {temp_xlsx}")
   return temp_xlsx

# -------------------------
# Helpers
# -------------------------
def find_report_range_row(df: pd.DataFrame, max_rows: int = 10) -> Optional[int]:
   month_pattern = re.compile(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b', re.IGNORECASE)
   for i in range(min(max_rows, len(df))):
       row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()]).strip()
       if "to" in row_text.lower() and month_pattern.search(row_text):
           return i
   return None

def parse_month_year_from_range(text: str) -> Optional[datetime]:
   # Try format: "01-Jan-2026" or "1-Jan-2026"
   m = re.search(r'(\d{1,2})[-/](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-/](\d{4})', text, re.IGNORECASE)
   if m:
       try:
           day = int(m.group(1))
           month_str = m.group(2)
           year = int(m.group(3))
           return datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
       except Exception:
           pass
   # Try format: "Jan 01, 2026" or "Jan 1 2026"
   m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s*\d{1,2}\,?\s*\d{4}', text, re.IGNORECASE)
   if m:
       try:
           return datetime.strptime(m.group(0).replace('.', '').replace(',', ''), "%b %d %Y")
       except Exception:
           pass
   # Try format: "Jan 2026"
   m2 = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s*\d{4}', text, re.IGNORECASE)
   if m2:
       try:
           return datetime.strptime(m2.group(0).replace('.', ''), "%b %Y")
       except Exception:
           pass
   return None

def detect_date_row(df: pd.DataFrame, start_search: int = 0, max_rows: int = 20) -> Optional[int]:
   # Pattern for dates like "01-Jan", "02-Jan" or just "01", "02"
   date_pattern = re.compile(r'^\s*(\d{1,2})(?:[-/](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))?\b', re.IGNORECASE)
   for r in range(start_search, min(len(df), max_rows)):
       row = df.iloc[r].astype(str).fillna("").tolist()
       day_count = sum(1 for cell in row if date_pattern.match(cell))
       if day_count >= 1:
           print(f"[detect_date_row] Found likely date row at index {r} (day_count={day_count})")
           return r
   print("[detect_date_row] Could not find date row by heuristic")
   return None

def build_date_map(date_row: List[object], month_dt: datetime) -> Dict[int, str]:
   date_map = {}
   for idx, cell in enumerate(date_row):
       if pd.isna(cell):
           continue
       s = str(cell).strip()
       # Try format "01-Jan" or "01-Jan-2026"
       m = re.match(r'^\s*(\d{1,2})[-/](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:[-/](\d{4}))?\b', s, re.IGNORECASE)
       if m:
           day = int(m.group(1))
           month_str = m.group(2)
           year = int(m.group(3)) if m.group(3) else month_dt.year
           try:
               composed = datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
               date_map[idx] = composed.strftime("%Y-%m-%d")
           except Exception:
               continue
       else:
           # Fallback: try just day number like "01", "02"
           m2 = re.match(r'^\s*(\d{1,2})\b', s)
           if m2:
               day = int(m2.group(1))
               try:
                   composed = datetime(year=month_dt.year, month=month_dt.month, day=day)
                   date_map[idx] = composed.strftime("%Y-%m-%d")
               except Exception:
                   continue
   print(f"[build_date_map] Built date_map for {len(date_map)} day columns")
   return date_map

def format_timestamp(date_str: str, time_val, is_checkin=True):
   """Combine date and time into 'YYYY-MM-DD HH:MM:00' 24-hour format."""
   if pd.isna(time_val) or str(time_val).strip() == "":
       return None

   # Try to parse numeric Excel time
   if isinstance(time_val, (float, int)):
       # If value looks like HH.MM (e.g., 17.45) instead of Excel fraction
       if time_val > 1: 
         hours = int(time_val)
         minutes = int(round((time_val - hours) * 100))  # .45 -> 45 mins
       else:
         total_seconds = int(float(time_val) * 24 * 3600)
         hours = (total_seconds // 3600) % 24
         minutes = (total_seconds % 3600) // 60
   else:
       # String like "9.28" or "09:28" or "9:28 AM"
       t_str = str(time_val).strip().replace('.', ':')
       try:
           # Try 24-hour first
           parsed = datetime.strptime(t_str, "%H:%M")
           hours = parsed.hour
           minutes = parsed.minute
       except ValueError:
           try:
               # Try 12-hour with AM/PM
               parsed = datetime.strptime(t_str, "%I:%M %p")
               hours = parsed.hour
               minutes = parsed.minute
           except ValueError:
               # Fallback: split by :
               parts = re.split(r'[:]', t_str)
               hours = int(parts[0]) if parts[0].isdigit() else 0
               minutes = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

   # Default if parsing failed completely
   if hours == 0 and minutes == 0:
       hours = 9 if is_checkin else 17  # 9:00 for in, 17:00 for out

   return f"{date_str} {hours:02d}:{minutes:02d}:00"


def calculate_working_hours(in_time: Optional[str], out_time: Optional[str]) -> float:
    """
    Calculate working hours between in_time and out_time.
    Handles overnight shifts.
    Returns hours in decimal format.
    """
    if not in_time or not out_time:
        return 0.0

    try:
        in_dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        out_dt = datetime.strptime(out_time, "%Y-%m-%d %H:%M:%S")

        # Handle overnight shifts
        if out_dt < in_dt:
            out_dt = out_dt + timedelta(days=1)

        diff = out_dt - in_dt
        hours = diff.total_seconds() / 3600

        return round(hours, 2)
    except Exception as e:
        print(f"[calculate_working_hours] Error: {e}")
        return 0.0


def format_working_hours(hours: float) -> str:
    """Convert decimal hours to HH:MM format"""
    if hours <= 0:
        return "00:00"

    h = int(hours)
    m = int((hours - h) * 60)
    return f"{h:02d}:{m:02d}"


def calculate_overtime(work_hours: float) -> str:
    """
    Calculate overtime.
    Formula: OT = Working Hours - 9
    Returns blank if OT < 0 hour
    """
    if not work_hours or work_hours <= 0:
        return ""

    shift_hours = 9
    overtime = round(work_hours - shift_hours, 2)

    if overtime <= 0:
        return ""

    return str(overtime)


# -------------------------
# Main function
# -------------------------
def clean_crystal_excel(input_path: str, output_path: str, company: str = None, branch: str = None,
                       header_keywords: List[str] = None, min_nonempty_ratio: float = 0.35) -> pd.DataFrame:
   print("=" * 80)
   print("[clean_crystal_excel] Starting")
   print(f"[clean_crystal_excel] Input: {input_path}")
   print(f"[clean_crystal_excel] Output: {output_path}")
   print(f"[clean_crystal_excel] Company: {company}")
   print(f"[clean_crystal_excel] Branch: {branch}")
   print("=" * 80)

   if not os.path.exists(input_path):
       raise FileNotFoundError(f"Input file not found: {input_path}")

   working_file = input_path
   temp_created = False
   if input_path.lower().endswith(".xls"):
       working_file = convert_xls_to_xlsx(input_path)
       temp_created = True

   df_raw = pd.read_excel(working_file, header=None, engine="openpyxl", dtype=object)
   print(f"[clean_crystal_excel] Loaded raw DataFrame shape: {df_raw.shape}")

   range_row_idx = find_report_range_row(df_raw, max_rows=6)
   month_dt = None
   if range_row_idx is not None:
       range_text = " ".join(df_raw.iloc[range_row_idx].dropna().astype(str).tolist())
       month_dt = parse_month_year_from_range(range_text)
       print(f"[clean_crystal_excel] Found range row {range_row_idx}: '{range_text}' -> month_dt={month_dt}")
   else:
       print("[clean_crystal_excel] WARNING: report range row not found; will use current month")
       month_dt = datetime.today()

   search_start = range_row_idx + 1 if range_row_idx is not None else 0
   date_row_idx = detect_date_row(df_raw, start_search=search_start, max_rows=40)
   if date_row_idx is None:
       if 5 < len(df_raw):
           print("[clean_crystal_excel] Trying fallback row index 5 for date row")
           date_row_idx = 5
       else:
           raise ValueError("Could not detect date row containing day labels")

   date_row = df_raw.iloc[date_row_idx].tolist()
   date_map = build_date_map(date_row, month_dt)

   records = []
   r = date_row_idx + 1
   total_rows = len(df_raw)

   status_map = {
       "H": "Holiday",
       "HO": "Holiday",
       "WO": "Holiday",
       "A": "Absent",
       "P": "Present",
       "½P": "Half Day",
       "0.5P": "Half Day",
       ".5P": "Half Day",
       "HD": "Half Day",
       "HALF": "Half Day",
       "H½P": "Half Day",
       "½BH": "Half Day",
       "T": "Terminated",
       "ML": "On Leave",
       "CO": "On Leave",
       "HP": "Half Day",
   }

   # Statuses to exclude from records
   excluded_statuses = ["Holiday", "Terminated"]

   while r < total_rows:
       row0 = df_raw.iloc[r].astype(object).tolist()

       emp_code_label_col = None
       for c, cell in enumerate(row0):
           if isinstance(cell, str) and re.match(r'^\s*Emp(loyee)?\.?\s*Code', cell, re.IGNORECASE):
               emp_code_label_col = c
               break

       if emp_code_label_col is None:
           r += 1
           continue

       emp_code = None
       for cc in range(emp_code_label_col + 1, len(row0)):
           candidate = row0[cc]
           if pd.notna(candidate) and str(candidate).strip() != "":
               emp_code = str(candidate).strip()
               break

       emp_name = None
       for c, cell in enumerate(row0):
           if isinstance(cell, str) and re.match(r'^\s*Emp(loyee)?\.?\s*Name', cell, re.IGNORECASE):
               for cc in range(c + 1, len(row0)):
                   candidate = row0[cc]
                   if pd.notna(candidate) and str(candidate).strip() != "":
                       emp_name = str(candidate).strip()
                       break
               break

       if not emp_name:
           candidates = [(c_idx, str(val).strip()) for c_idx, val in enumerate(row0)
                         if pd.notna(val) and isinstance(val, str) and len(str(val).strip()) > 3]
           if candidates:
               for c_idx, txt in candidates:
                   if not re.match(r'^(Days|Status|InTime|OutTime|Total)$', txt, re.IGNORECASE) and not re.match(r'Emp', txt, re.IGNORECASE):
                       emp_name = txt
                       break

       status_row = None
       intime_row = None
       outtime_row = None
       search_end = min(total_rows, r + 20)  # Increased from 12 for more robustness
       for r2 in range(r + 1, search_end):
           row_vals = df_raw.iloc[r2].astype(object).tolist()
           first_texts = [str(x).strip() if pd.notna(x) else "" for x in row_vals[:6]]
           if any(re.match(r'^\s*Status\s*$', t, re.IGNORECASE) for t in first_texts):
               status_row = df_raw.iloc[r2]
           if any(re.match(r'^\s*In\s*Time\s*$', t, re.IGNORECASE) for t in first_texts):
               intime_row = df_raw.iloc[r2]
           if any(re.match(r'^\s*Out\s*Time\s*$', t, re.IGNORECASE) for t in first_texts):
               outtime_row = df_raw.iloc[r2]
           if status_row is not None and intime_row is not None and outtime_row is not None:
               break

       if status_row is None:
           r += 1
           continue

       for col_idx, date_str in date_map.items():
           raw_status = status_row.iloc[col_idx] if col_idx < len(status_row) else None
           if pd.isna(raw_status) or str(raw_status).strip() == "":
               continue
           raw_status_s = str(raw_status).strip()
           status_final = status_map.get(raw_status_s, raw_status_s)

           # Skip records with excluded statuses
           if status_final in excluded_statuses:
               print(f"[clean_crystal_excel] Skipping {status_final} record for {emp_name} on {date_str}")
               continue

           check_in = None
           if intime_row is not None and col_idx < len(intime_row):
               check_in = format_timestamp(date_str, intime_row.iloc[col_idx], is_checkin=True)

           check_out = None
           if outtime_row is not None and col_idx < len(outtime_row):
               check_out = format_timestamp(date_str, outtime_row.iloc[col_idx], is_checkin=False)

           # Calculate working hours from In Time and Out Time
           work_hours_decimal = calculate_working_hours(check_in, check_out)
           work_hours_formatted = format_working_hours(work_hours_decimal)

           # Calculate overtime (OT = Working Hours - 9, blank if < 1 hour)
           overtime_val = calculate_overtime(work_hours_decimal)

           # Use ERPNext Attendance field labels as columns
           rec = {
               "Attendance Date": date_str,
               "Employee": emp_code if emp_code else "",  # Assuming employee code is the Employee ID
               "Employee Name": emp_name if emp_name else "",
               "Status": status_final,
               "In Time": check_in,
               "Out Time": check_out,
               "Working Hours": work_hours_formatted,
               "Over Time": overtime_val,
               "Company": company if company else "Vaaman Engineers India Limited",
               "Branch": branch if branch else "",
           }
           records.append(rec)

       found_next_emp = False
       for look_r in range(r + 1, min(total_rows, r + 20)):
           rowlook = df_raw.iloc[look_r].astype(str).fillna("").tolist()
           if any(re.match(r'^\s*Emp(loyee)?\.?\s*Code', str(x), re.IGNORECASE) for x in rowlook[:15]):
               r = look_r
               found_next_emp = True
               break
       if not found_next_emp:
           r += 1

   # Use ERPNext Attendance field labels as columns
   final_cols = ["Attendance Date", "Employee", "Employee Name", "Status", "In Time", "Out Time", "Working Hours", "Over Time", "Company", "Branch"]
   df_final = pd.DataFrame.from_records(records, columns=final_cols)
    # Remove any rows where all critical fields are empty
   df_final = df_final.dropna(subset=["Attendance Date", "Employee", "Status"], how="all")
   print(f"[clean_crystal_excel] Built final DataFrame with {len(df_final)} rows (after filtering out Holiday/Terminated)")

   if df_final.empty:
       raise ValueError("No attendance records parsed from the report. Check input file format.")

   out_dir = os.path.dirname(output_path)
   if out_dir and not os.path.exists(out_dir):
       os.makedirs(out_dir, exist_ok=True)

   df_final.to_excel(output_path, index=False)
   print(f"[clean_crystal_excel] Saved output to: {output_path}")

   if temp_created and os.path.exists(working_file):
       try:
           os.unlink(working_file)
           print(f"[clean_crystal_excel] Removed temporary file: {working_file}")
       except Exception:
           pass

   print("[clean_crystal_excel] Done ✅")
   return df_final