# clean_daily_inout4.py

import os
import re
import tempfile
from datetime import datetime
from typing import Dict, Optional, Tuple, List

import frappe
import pandas as pd
import xlrd
from openpyxl import Workbook


# =========================
#  .xls -> .xlsx conversion
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


# =========
# Helpers
# =========
def parse_period_month(df: pd.DataFrame, max_rows: int = 15) -> datetime:
    """
    Parse month/year from header like:
    'Performance Register from 01/07/2025 to 31/07/2025'
    """
    for i in range(min(max_rows, len(df))):
        row_text = " ".join([str(x) for x in df.iloc[i].dropna().astype(str).tolist()])
        clean_text = row_text.replace("\xa0", " ")  # replace non-breaking space

        # DEBUG log before regex
        print(f"[DEBUG parse_period_month] Row {i} raw: {row_text!r}")
        print(f"[DEBUG parse_period_month] Row {i} cleaned: {clean_text!r}")

        m = re.search(
            r'from\s+(\d{1,2}[/-]\d{1,2}[/-]\d{4})\s+to\s+(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
            clean_text,
            re.IGNORECASE,
        )

        # DEBUG log after regex
        if m:
            print(f"[DEBUG parse_period_month] Row {i} -> regex matched groups: {m.groups()}")
            try:
                dt = datetime.strptime(m.group(1).replace("-", "/"), "%d/%m/%Y")
                print(f"[parse_period_month] Using month from: {m.group(1)} -> {dt:%Y-%m}")
                return dt
            except Exception as e:
                print(f"[ERROR parse_period_month] Failed to parse date {m.group(1)}: {e}")
        else:
            print(f"[DEBUG parse_period_month] Row {i} -> regex did not match")

    # fallback: today
    today = datetime.today()
    print(f"[parse_period_month] Period not found in first {max_rows} rows. Using today: {today:%Y-%m}")
    return today



def detect_shift(in_time: Optional[str], out_time: Optional[str]) -> str:
    """
    Decide shift code based on in_time/out_time.
    Shifts:
      G: 10-17
      A: 6-14
      B: 14-22
      C: 22-6 (overnight)
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

    # prefer in_time if available, else out_time
    hour = in_hour if in_hour is not None else out_hour
    if hour is None:
        return "G"  # default fallback

    if 6 <= hour < 14:
        return "A"
    elif 14 <= hour < 22:
        return "B"
    elif hour >= 22 or hour < 6:
        return "C"
    elif 10 <= hour < 17:
        return "G"
    return "G"


def detect_date_row(df: pd.DataFrame, start: int = 0, max_check: int = 80) -> Optional[int]:
   """
   Find the row that has many day numbers (01..31). Heuristic: >= 6 day tokens.
   """
   for r in range(start, min(len(df), max_check)):
       vals = df.iloc[r].astype(str).fillna("").tolist()
       day_count = 0
       for v in vals:
           s = v.strip()
           if re.fullmatch(r'0?\d|[12]\d|3[01]', s):  # 0..9, 10..29, 30..31 with or without leading 0
               day_count += 1
       if day_count >= 6:
           print(f"[detect_date_row] Day row likely at index {r} (day_count={day_count})")
           return r
   print("[detect_date_row] Could not locate day row by heuristic")
   return None


def build_date_map(date_row: pd.Series, month_dt: datetime) -> Dict[int, str]:
   """
   Map sheet column index -> 'YYYY-MM-DD' for columns that carry a day number.
   """
   date_map: Dict[int, str] = {}
   for idx, cell in enumerate(date_row.tolist()):
       if pd.isna(cell):
           continue
       s = str(cell).strip()
       if re.fullmatch(r'0?\d|[12]\d|3[01]', s):
           try:
               day = int(s)
               dt = datetime(year=month_dt.year, month=month_dt.month, day=day)
               date_map[idx] = dt.strftime("%Y-%m-%d")
           except Exception:
               continue
   print(f"[build_date_map] Built mapping for {len(date_map)} day columns")
   return date_map


def format_timestamp(date_str: str, time_val, is_checkin: bool) -> Optional[str]:
   """
   Return 'YYYY-MM-DD HH:MM:00' or None if empty. Handles:
   - Excel numeric time (fraction of day)
   - 'HH:MM', 'H:MM', 'HH.MM', 'H.MM'
   - 'HH:MM AM/PM'
   """
   if time_val is None or (isinstance(time_val, float) and pd.isna(time_val)) or str(time_val).strip() == "":
       return None
   try:
       if isinstance(time_val, (float, int)):
           total_seconds = int(float(time_val) * 24 * 3600)
           hours = (total_seconds // 3600) % 24
           minutes = (total_seconds % 3600) // 60
           return f"{date_str} {hours:02d}:{minutes:02d}:00"
       t = str(time_val).strip().replace(".", ":")
       # 24h
       try:
           dt = datetime.strptime(t, "%H:%M")
           return f"{date_str} {dt.hour:02d}:{dt.minute:02d}:00"
       except Exception:
           pass
       # 12h
       try:
           dt = datetime.strptime(t, "%I:%M %p")
           return f"{date_str} {dt.hour:02d}:{dt.minute:02d}:00"
       except Exception:
           pass
       # last-resort: split
       parts = [p for p in t.split(":") if p != ""]
       h = int(parts[0]) if parts and parts[0].isdigit() else (9 if is_checkin else 17)
       m = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
       return f"{date_str} {h:02d}:{m:02d}:00"
   except Exception:
       # default if unparsable
       return f"{date_str} {'09:00:00' if is_checkin else '17:00:00'}"


def map_status(raw_status) -> str:
   s = "" if pd.isna(raw_status) else str(raw_status).strip()
   mapping = {
       "P": "Present", "POW": "Present", "POH": "Present", "PWH": "Present",
       "A": "Absent", "A1": "Absent",
       "WO": "Holiday", "H": "Holiday",  "HLD": "Holiday", "WOH": "Holiday",
       "CL": "On Leave", "PL": "On Leave", "SL": "On Leave", "EL": "On Leave", "RL": "On Leave", "LWP": "On Leave", "SDL": "On Leave", "QL": "On Leave", "TU": "On Leave", "CO": "On Leave", "TR": "On Leave", "OH": "On Leave", "ML": "On Leave", "CH": "On Leave", "SCL": "On Leave", "SPL": "On Leave",
       "MIS": "Half Day",  "HD": "Half Day", "HALF": "Half Day",
       "WFH": "Work From Home"
   }
   return mapping.get(s, s)


def extract_gp_and_name_from_gprow(row_cells):
    """
    Scan a GP row (list of cell values) and extract (gp_no, emp_name).
    Example: ['GP No. & NAME', nan, nan, 'PMP0005515, Naveen Singh Rana', ...]
             → ("PMP0005515", "Naveen Singh Rana")
    """
    gp_no = None
    emp_name = None

    for cell in row_cells:
        if not cell or not isinstance(cell, str):
            continue

        # Match GP number followed by comma + name
        m = re.match(r"([A-Z0-9]+)\s*,\s*(.+)", cell.strip())
        if m:
            gp_no, emp_name = m.group(1), m.group(2).strip()
            break  # stop at the first match

    return gp_no, emp_name



# ========
#  Main
# ========
def clean_daily_inout4(input_path: str, output_path: str, company: str = None, branch: str = None) -> pd.DataFrame:
   print("=" * 80)
   print("[clean_daily_inout4] Starting")
   print(f"[clean_daily_inout4] Input: {input_path}")
   print(f"[clean_daily_inout4] Output: {output_path}")
   print(f"[clean_daily_inout4] Company: {company}")
   print(f"[clean_daily_inout4] Branch: {branch}")
   print("=" * 80)

   if not os.path.exists(input_path):
       raise FileNotFoundError(f"Input file not found: {input_path}")

   working_file = input_path
   temp_created = False
   if input_path.lower().endswith(".xls"):
       working_file = convert_xls_to_xlsx(input_path)
       temp_created = True

   # read raw
   df = pd.read_excel(working_file, header=None, engine="openpyxl", dtype=object)
   print(f"[clean_daily_inout4] Raw shape: {df.shape}")

   # 1) Month/year
   month_dt = parse_period_month(df)

   # 2) Date row (01..31)
   date_row_idx = detect_date_row(df, start=0, max_check=80)
   if date_row_idx is None:
       # try a broader search
       date_row_idx = detect_date_row(df, start=0, max_check=len(df))
       if date_row_idx is None:
           raise ValueError("Could not find a row with day numbers (01..31)")

   date_map = build_date_map(df.iloc[date_row_idx], month_dt)
   if not date_map:
       raise ValueError("Date columns could not be mapped from the day row")

   # 3) Scan for each GP block: rows containing 'GP No. & NAME'
   gp_header_regex = re.compile(r'GP\s*No\.?\s*&?\s*NAME', re.IGNORECASE)
   gp_rows: List[int] = []
   for r in range(len(df)):
       row_text = " ".join([str(x) for x in df.iloc[r].dropna().astype(str).tolist()])
       if gp_header_regex.search(row_text):
           gp_rows.append(r)

   if not gp_rows:
       raise ValueError("No 'GP No. & NAME' row found anywhere in the sheet")

   print(f"[clean_daily_inout4] Found {len(gp_rows)} GP header rows at indices: {gp_rows}")
   if not gp_rows:
    print("[DEBUG] No GP rows found. Check header text in your file, regex may need adjusting.")


   # 4) Build records
   records = []
   for base in gp_rows:
       # According to your pattern the block height is 11 rows, with offsets:
       # GP No. & NAME -> base
       # IN1           -> base + 2
       # Out2          -> base + 5
       # H Work        -> base + 6
       # OT            -> base + 7
       # Status        -> base + 9
       # (Example: 13,15,18,19,20,22 and again 24,26,29,30,31,33)
       print(f"[DEBUG] Processing GP block at row {base}")


       def get_row_safe(ridx: int) -> Optional[pd.Series]:
           if 0 <= ridx < len(df):
               return df.iloc[ridx]
           return None

       gp_row = get_row_safe(base)
       in1_row = get_row_safe(base + 2)
       out2_row = get_row_safe(base + 5)
       h_row = get_row_safe(base + 6)
       ot_row = get_row_safe(base + 7)
       status_row = get_row_safe(base + 9)

       if gp_row is None or in1_row is None or out2_row is None or h_row is None or status_row is None:
           print(f"[clean_daily_inout4] Skipping block at {base} (incomplete rows)")
           continue

       gp_no, emp_name = extract_gp_and_name_from_gprow(gp_row.tolist())
       print(f"[DEBUG] Extracted gp_no={gp_no}, emp_name={emp_name}")

       if not gp_no:
           print(f"[clean_daily_inout4] Could not extract GP number at block {base}, skipping")
           print(f"[DEBUG] Skipping block {base}, no GP number extracted from row: {gp_row.tolist()}")
           continue

       # Resolve Employee ID in ERPNext
       try:
           # emp_doc = frappe.get_doc("Employee", {"attendance_device_id": gp_no})
           emp_code = frappe.db.get_value("Employee", {"attendance_device_id": gp_no}, "name")
           if not emp_code:
               emp_code = ""
       except Exception:
           emp_code = ""  # fallback if not found

       # nearest likely day row is above the block; use the global one we already built (date_map)
       for col_idx, date_str in date_map.items():
           # per-day values from aligned columns
           in1_val = in1_row.iloc[col_idx] if col_idx < len(in1_row) else None
           out2_val = out2_row.iloc[col_idx] if col_idx < len(out2_row) else None
           h_val = h_row.iloc[col_idx] if col_idx < len(h_row) else None
           ot_val = ot_row.iloc[col_idx] if (ot_row is not None and col_idx < len(ot_row)) else None
           st_val = status_row.iloc[col_idx] if col_idx < len(status_row) else None

           # skip fully empty day
           if (pd.isna(in1_val) or str(in1_val).strip() == "") and \
              (pd.isna(out2_val) or str(out2_val).strip() == "") and \
              (pd.isna(st_val) or str(st_val).strip() == "") and \
              (pd.isna(h_val) or str(h_val).strip() == "") and \
              (ot_row is None or pd.isna(ot_val) or str(ot_val).strip() == ""):
               print(f"[DEBUG] Skipping {gp_no} {emp_name} on {date_str} (all values empty)")
               continue

           status_mapped = map_status(st_val)

            # skip holidays
        #    if status_mapped == "Holiday":
        #        print(f"[DEBUG] Skipping {gp_no} {emp_name} on {date_str} (Holiday)")
        #        continue

           rec = {
               "Attendance Date": date_str,
            #    "Gate Pass No.": gp_no, 
               "Employee": emp_code,                      # ERPNext Employee ID (resolved)
               "Employee Name": emp_name if emp_name else "",  # just the name
               "Status": map_status(st_val),
               "In Time": format_timestamp(date_str, in1_val, is_checkin=True),
               "Out Time": format_timestamp(date_str, out2_val, is_checkin=False),
               "Working Hours": "" if pd.isna(h_val) else str(h_val).strip(),
               "Over Time": "" if (ot_row is None or pd.isna(ot_val)) else str(ot_val).strip(),
               "Shift": detect_shift(
                    format_timestamp(date_str, in1_val, is_checkin=True),
                    format_timestamp(date_str, out2_val, is_checkin=False),
                ),
               "Company": company if company else "Vaaman Engineers India Limited",
               "Branch": branch if branch else "",
           }
           records.append(rec)

   df_final = pd.DataFrame.from_records(
       records,
       columns=[
           "Attendance Date",
        #    "Gate Pass No.",
           "Employee",
           "Employee Name",
           "Status",
           "In Time",
           "Out Time",
           "Working Hours",
           "Over Time",
           "Shift", 
           "Company",
           "Branch",
       ],
   )

   if df_final.empty:
       raise ValueError( "❌ No attendance records could be parsed. "
        "Please check that the uploaded file matches the selected Branch "
        "and that the file format is correct.")

   # save
   out_dir = os.path.dirname(output_path)
   if out_dir and not os.path.exists(out_dir):
       os.makedirs(out_dir, exist_ok=True)
   df_final.to_excel(output_path, index=False)
   print(f"[clean_daily_inout4] Saved cleaned file: {output_path}")

   # cleanup temp
   if temp_created and os.path.exists(working_file):
       try:
           os.unlink(working_file)
           print(f"[clean_daily_inout4] Removed temp: {working_file}")
       except Exception:
           pass

   print("[clean_daily_inout4] Done ✅")
   return df_final
