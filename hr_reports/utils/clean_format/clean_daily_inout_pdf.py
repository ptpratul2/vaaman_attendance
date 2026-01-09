# clean_daily_inout_pdf.py
"""
Cleaner for ManHour Report (Vertical PDF format - HSM-2 Project / AMNS Surat).

Key behaviour:
- Vertical format PDF with one row per attendance entry
- Columns: Contractor, Workmen, IDNo, Workorder No, Workskill, Card Type, Shift, Date, In Time, Out Time, Man Days, Man Hrs, OT, Status
- Converts PDF to DataFrame using tabula-py or pdfplumber
- Maps IDNo to Employee using attendance_device_id
- CALCULATES working hours from In Time to Out Time (ignores PDF Man Hrs column)
- Determines status based on calculated working hours:
  * >= 7.0 hours → Present
  * >= 4.5 hours → Half Day
  * < 4.5 hours → Absent
  * No punch → Absent
- Auto-detects shift from In Time (ignores PDF Shift column):
  * A shift: 05:00 - 07:00
  * G shift: 08:00 - 10:00
  * B shift: 13:00 - 15:00
  * C shift: 21:00 - 23:00
- Calculates overtime: OT = Calculated Hours - 9 (only shown if >= 1 hour)
"""

import os
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import frappe
import pandas as pd

# PDF extraction libraries (install: pip install tabula-py pdfplumber)
try:
    import tabula
    TABULA_AVAILABLE = True
except ImportError:
    TABULA_AVAILABLE = False
    print("Warning: tabula-py not installed. Install with: pip install tabula-py")

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False
    print("Warning: pdfplumber not installed. Install with: pip install pdfplumber")


# -------------------------
# PDF Extraction Functions
# -------------------------
def extract_pdf_with_tabula(pdf_path: str) -> pd.DataFrame:
    """
    Extract table from PDF using tabula-py.
    Returns DataFrame with all pages combined.
    """
    if not TABULA_AVAILABLE:
        raise ImportError("tabula-py is required. Install with: pip install tabula-py")
    
    print(f"[extract_pdf_with_tabula] Extracting tables from: {pdf_path}")
    
    # Read all pages
    tables = tabula.read_pdf(
        pdf_path,
        pages='all',
        multiple_tables=True,
        pandas_options={'header': None}
    )
    
    if not tables:
        raise ValueError("No tables found in PDF")
    
    # Combine all tables
    df_combined = pd.concat(tables, ignore_index=True)
    print(f"[extract_pdf_with_tabula] Extracted {len(df_combined)} rows")
    
    return df_combined


def extract_pdf_with_pdfplumber(pdf_path: str) -> pd.DataFrame:
    """
    Extract table from PDF using pdfplumber.
    Returns DataFrame with all pages combined.
    """
    if not PDFPLUMBER_AVAILABLE:
        raise ImportError("pdfplumber is required. Install with: pip install pdfplumber")
    
    print(f"[extract_pdf_with_pdfplumber] Extracting tables from: {pdf_path}")
    
    all_rows = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            table = page.extract_table()
            if table:
                all_rows.extend(table)
                print(f"[extract_pdf_with_pdfplumber] Page {page_num}: {len(table)} rows")
    
    if not all_rows:
        raise ValueError("No tables found in PDF")
    
    df = pd.DataFrame(all_rows)
    print(f"[extract_pdf_with_pdfplumber] Total extracted: {len(df)} rows")
    
    return df


def extract_pdf_to_dataframe(pdf_path: str, method: str = "auto") -> pd.DataFrame:
    """
    Extract PDF to DataFrame using the specified method.
    
    Args:
        pdf_path: Path to PDF file
        method: "tabula", "pdfplumber", or "auto" (tries both)
    
    Returns:
        DataFrame with extracted data
    """
    if method == "auto":
        # Try pdfplumber first (more reliable for complex tables)
        if PDFPLUMBER_AVAILABLE:
            try:
                return extract_pdf_with_pdfplumber(pdf_path)
            except Exception as e:
                print(f"[extract_pdf_to_dataframe] pdfplumber failed: {e}")
        
        # Fallback to tabula
        if TABULA_AVAILABLE:
            try:
                return extract_pdf_with_tabula(pdf_path)
            except Exception as e:
                print(f"[extract_pdf_to_dataframe] tabula failed: {e}")
        
        raise ImportError("No PDF extraction library available. Install tabula-py or pdfplumber")
    
    elif method == "tabula":
        return extract_pdf_with_tabula(pdf_path)
    
    elif method == "pdfplumber":
        return extract_pdf_with_pdfplumber(pdf_path)
    
    else:
        raise ValueError(f"Invalid method: {method}. Use 'tabula', 'pdfplumber', or 'auto'")


# -------------------------
# Helper Functions
# -------------------------
def identify_header_row(df: pd.DataFrame) -> int:
    """
    Find the header row by looking for key column names.
    Returns row index of header.
    """
    keywords = ["Workmen", "IDNo", "Date", "In Time", "Out Time", "Status"]
    
    for idx in range(min(10, len(df))):
        row_text = " ".join([str(x).strip() for x in df.iloc[idx].dropna().tolist()])
        
        # Check if this row contains header keywords
        matches = sum(1 for kw in keywords if kw.lower() in row_text.lower())
        if matches >= 4:
            print(f"[identify_header_row] Found header at row {idx}")
            return idx
    
    print("[identify_header_row] Header not found, assuming row 0")
    return 0


def normalize_user_id(user_id: str) -> str:
    """
    Normalize User ID by removing leading zeros.
    Examples:
    - 11000346434 → 11000346434 (no change)
    - 0011000346 → 11000346
    """
    if not user_id or pd.isna(user_id):
        return ""
    
    user_id_str = str(user_id).strip()
    
    # Remove leading zeros
    normalized = user_id_str.lstrip('0')
    
    # If all zeros, return single zero
    if not normalized:
        return "0"
    
    return normalized


def parse_date(date_str: str) -> Optional[str]:
    """
    Parse date from various formats.
    Examples:
    - 06/12/2025 → 2025-12-06
    - 01/12/2025 → 2025-12-01
    
    Returns: YYYY-MM-DD format
    """
    if not date_str or pd.isna(date_str):
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # Try DD/MM/YYYY format
        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                day, month, year = parts
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%Y-%m-%d")
        
        # Try DD-MM-YYYY format
        if "-" in date_str:
            parts = date_str.split("-")
            if len(parts) == 3:
                day, month, year = parts
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%Y-%m-%d")
        
        return None
    except Exception as e:
        print(f"[parse_date] Error parsing '{date_str}': {e}")
        return None


def parse_time_to_datetime(date_str: str, time_str: str) -> Optional[str]:
    """
    Parse time and combine with date.
    
    Examples:
    - "10:44 AM" → "2025-12-06 10:44:00"
    - "07:25 PM" → "2025-12-06 19:25:00"
    
    Returns: YYYY-MM-DD HH:MM:SS format
    """
    if not time_str or pd.isna(time_str) or not date_str:
        return None
    
    try:
        time_str = str(time_str).strip()
        
        # Handle 12-hour format with AM/PM
        if "AM" in time_str.upper() or "PM" in time_str.upper():
            # Remove AM/PM and parse
            is_pm = "PM" in time_str.upper()
            time_clean = re.sub(r'[APM\s]', '', time_str, flags=re.IGNORECASE)
            
            parts = time_clean.split(":")
            if len(parts) >= 2:
                hour = int(parts[0])
                minute = int(parts[1])
                
                # Convert to 24-hour format
                if is_pm and hour != 12:
                    hour += 12
                elif not is_pm and hour == 12:
                    hour = 0
                
                return f"{date_str} {hour:02d}:{minute:02d}:00"
        
        # Handle 24-hour format
        if ":" in time_str:
            parts = time_str.split(":")
            if len(parts) >= 2:
                hour = int(parts[0])
                minute = int(parts[1])
                return f"{date_str} {hour:02d}:{minute:02d}:00"
        
        return None
    except Exception as e:
        print(f"[parse_time_to_datetime] Error parsing time '{time_str}': {e}")
        return None


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


def determine_status(working_hours: float, status_code: str = "") -> str:
    """
    Determine attendance status based on working hours and status code.
    
    Status codes from file:
    - P: Present
    - SP: Short Punch / Single Punch
    - HD: Half Day
    - WO: Weekly Off
    - PH: Public Holiday
    - PL: Paid Leave
    - LI: Leave
    - AB: Absent
    
    Logic:
    - If status_code is WO/PH/PL → "Present" or "On Leave"
    - If >= 7.0 hours → "Present"
    - If >= 4.5 hours → "Half Day"
    - If < 4.5 hours → "Absent"
    """
    code = str(status_code).strip().upper() if pd.notna(status_code) else ""
    
    # Handle special status codes
    status_map = {
        "WO": "Present",      # Weekly Off
        "PH": "On Leave",     # Public Holiday
        "PL": "On Leave",     # Paid Leave
        "LI": "On Leave",     # Leave
        "AB": "Absent",       # Absent
    }
    
    if code in status_map:
        return status_map[code]
    
    # Determine by working hours
    if working_hours >= 7.0:
        return "Present"
    elif working_hours >= 4.5:
        return "Half Day"
    else:
        return "Absent"


def detect_shift_from_time(in_time: Optional[str]) -> str:
    """
    Detect shift based on IN time (similar to clean_daily_inout16).

    Shift timings:
    - A shift: 05:00 - 07:00
    - G shift: 08:00 - 10:00 (General day shift)
    - B shift: 13:00 - 15:00
    - C shift: 21:00 - 23:00 (Night shift)
    """
    if not in_time or str(in_time).strip() == "":
        return ""

    try:
        in_dt = datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
        hour = in_dt.hour

        if 5 <= hour <= 7:
            return "A"
        elif 8 <= hour <= 10:
            return "G"
        elif 13 <= hour <= 15:
            return "B"
        elif 21 <= hour <= 23:
            return "C"
        else:
            # Find nearest shift
            distances = {
                "A": abs(hour - 6),
                "G": abs(hour - 9),
                "B": abs(hour - 14),
                "C": abs(hour - 22) if hour > 12 else abs(hour + 24 - 22)
            }
            return min(distances, key=distances.get)
    except Exception:
        return ""


def calculate_overtime(work_hours: float) -> str:
    """
    Calculate overtime.
    Formula: OT = Working Hours - 9
    Returns blank if OT < 1 hour
    """
    if not work_hours or work_hours <= 0:
        return ""
    
    shift_hours = 9
    overtime = round(work_hours - shift_hours, 2)
    
    if overtime < 1:
        return ""
    
    return str(overtime)


# -------------------------
# Main Cleaning Function
# -------------------------
def clean_daily_inout_pdf(
    input_path: str,
    output_path: str,
    company: str = None,
    branch: str = None,
    pdf_method: str = "auto"
) -> pd.DataFrame:
    """
    Clean ManHour vertical PDF report.
    
    Args:
        input_path: Path to input PDF file
        output_path: Path to save cleaned Excel file
        company: Company name (default: from memory)
        branch: Branch name (optional)
        pdf_method: PDF extraction method ("tabula", "pdfplumber", or "auto")
    
    Returns:
        Cleaned DataFrame
    """
    print("=" * 80)
    print("[clean_daily_inout_pdf] Starting - ManHour Vertical Report (PDF)")
    print(f"[clean_daily_inout_pdf] Input: {input_path}")
    print(f"[clean_daily_inout_pdf] Output: {output_path}")
    print("=" * 80)
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Step 1: Extract PDF to DataFrame
    print("[clean_daily_inout_pdf] Step 1: Extracting PDF to DataFrame...")
    df_raw = extract_pdf_to_dataframe(input_path, method=pdf_method)
    print(f"[clean_daily_inout_pdf] Raw shape: {df_raw.shape}")
    
    # Step 2: Identify header row and set column names
    header_idx = identify_header_row(df_raw)

    # Set column names from header row
    if header_idx >= 0:
        df = df_raw.iloc[header_idx:].reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)

        # Clean column names - remove None, empty strings, newlines
        df.columns = [str(col).strip().replace('\n', ' ') if col and str(col).strip() else f"Col{i}"
                      for i, col in enumerate(df.columns)]

        print(f"[clean_daily_inout_pdf] Header found at row {header_idx}")
        print(f"[clean_daily_inout_pdf] Columns after header: {df.columns.tolist()}")
    else:
        df = df_raw.copy()
        # If no header found, use standard column names
        print(f"[clean_daily_inout_pdf] No header found, using standard column names")
        expected_cols = [
            "Project", "Contractor", "Workmen", "IDNo", "Workorder No", "Workskill",
            "Card Type", "Shift", "Date", "In Time", "Out Time",
            "Man Days", "Man Hrs", "OT", "Status"
        ]
        if len(df.columns) >= len(expected_cols):
            df.columns = expected_cols + [f"Col{i}" for i in range(len(df.columns) - len(expected_cols))]
        else:
            df.columns = expected_cols[:len(df.columns)]

    print(f"[clean_daily_inout_pdf] Final columns: {df.columns.tolist()}")
    print(f"[clean_daily_inout_pdf] Data rows: {len(df)}")

    # Check if Date column exists
    if "Date" not in df.columns:
        print("[clean_daily_inout_pdf] ⚠️ 'Date' column not found!")
        print(f"[clean_daily_inout_pdf] Available columns: {df.columns.tolist()}")
        print("[clean_daily_inout_pdf] Sample data:")
        print(df.head(3))
        raise ValueError("Date column not found in extracted data")

    # Step 3: Filter out header rows and empty rows
    print(f"[clean_daily_inout_pdf] Filtering rows where Date is not empty...")
    df = df[df["Date"].notna() & (df["Date"] != "Date")]
    df = df.reset_index(drop=True)
    print(f"[clean_daily_inout_pdf] After filtering: {len(df)} rows")
    
    # Step 4: Process each row
    records = []
    skipped_rows = 0
    processed_rows = 0

    print(f"[clean_daily_inout_pdf] Processing {len(df)} rows...")

    for idx, row in df.iterrows():
        try:
            # Extract fields
            contractor = str(row.get("Contractor", "")).strip()
            workmen_name = str(row.get("Workmen", "")).strip()
            id_no_raw = str(row.get("IDNo", "")).strip()
            workskill = str(row.get("Workskill", "")).strip()
            shift_raw = str(row.get("Shift", "")).strip()
            date_raw = str(row.get("Date", "")).strip()
            in_time_raw = str(row.get("In Time", "")).strip()
            out_time_raw = str(row.get("Out Time", "")).strip()
            man_hrs_raw = str(row.get("Man Hrs", "")).strip()
            ot_flag_raw = str(row.get("OT", "")).strip()
            status_raw = str(row.get("Status", "")).strip()

            # Skip if essential fields are missing
            if not date_raw or not id_no_raw:
                skipped_rows += 1
                if skipped_rows <= 5:  # Log first 5 skips for debugging
                    print(f"[clean_daily_inout_pdf] Row {idx} skipped: date_raw='{date_raw}', id_no_raw='{id_no_raw}'")
                continue
            
            # Normalize User ID
            user_id = normalize_user_id(id_no_raw)
            
            # Map User ID to Employee
            try:
                emp_code = frappe.db.get_value(
                    "Employee",
                    {"attendance_device_id": user_id},
                    "name"
                )
                if not emp_code:
                    emp_code = ""
                    print(f"[clean_daily_inout_pdf] Warning: No Employee found for User ID {user_id} (raw: {id_no_raw})")
            except Exception as e:
                emp_code = ""
                print(f"[clean_daily_inout_pdf] Error looking up User ID {user_id}: {e}")
            
            # Parse date
            date_str = parse_date(date_raw)
            if not date_str:
                skipped_rows += 1
                if skipped_rows <= 10:
                    print(f"[clean_daily_inout_pdf] Row {idx} skipped: failed to parse date '{date_raw}'")
                continue
            
            # Parse times
            in_time = parse_time_to_datetime(date_str, in_time_raw)
            out_time = parse_time_to_datetime(date_str, out_time_raw)

            # Calculate working hours from IN and OUT times (like clean_daily_inout16)
            work_hours_decimal = calculate_working_hours(in_time, out_time)
            work_hours_formatted = format_working_hours(work_hours_decimal)

            # Determine status based on calculated working hours (like clean_daily_inout16)
            if not in_time and not out_time:
                status = "Absent"
            elif work_hours_decimal == 0.0:
                status = "Absent"
            elif work_hours_decimal >= 7.0:
                status = "Present"
            elif work_hours_decimal >= 4.5:
                status = "Half Day"
            else:
                status = "Absent"

            # Detect shift from IN time (like clean_daily_inout16)
            shift_code = detect_shift_from_time(in_time)

            # Calculate overtime from calculated hours
            overtime = calculate_overtime(work_hours_decimal)
            
            # Build record
            record = {
                "Attendance Date": date_str,
                "Employee": emp_code,
                "Employee Name": workmen_name,
                "Status": status,
                "In Time": in_time or "",
                "Out Time": out_time or "",
                "Working Hours": work_hours_formatted,
                "Over Time": overtime,
                "Shift": shift_code,
                "Company": company if company else "Vaaman Engineers India Limited",
                "Branch": branch if branch else "",
            }

            records.append(record)
            processed_rows += 1

            # Log progress every 50 records
            if processed_rows % 50 == 0:
                print(f"[clean_daily_inout_pdf] Processed {processed_rows} records...")

        except Exception as e:
            print(f"[clean_daily_inout_pdf] Error processing row {idx}: {e}")
            skipped_rows += 1
            continue

    # Summary
    print(f"[clean_daily_inout_pdf] Processing complete:")
    print(f"  - Total rows after filter: {len(df)}")
    print(f"  - Successfully processed: {processed_rows}")
    print(f"  - Skipped/failed: {skipped_rows}")

    # Step 5: Create final DataFrame
    df_final = pd.DataFrame.from_records(
        records,
        columns=[
            "Attendance Date",
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
        error_msg = (
            f"❌ No attendance records could be parsed.\n"
            f"   - Total rows extracted from PDF: {len(df_raw)}\n"
            f"   - Rows after filtering: {len(df)}\n"
            f"   - Successfully processed: {processed_rows}\n"
            f"   - Skipped/failed: {skipped_rows}\n"
            f"   Please check:\n"
            f"   1. PDF file format matches expected structure\n"
            f"   2. Date and IDNo columns contain valid data\n"
            f"   3. Review the processing logs above for specific errors"
        )
        raise ValueError(error_msg)
    
    print(f"[clean_daily_inout_pdf] Total records parsed: {len(df_final)}")
    
    # Step 6: Save output
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    
    df_final.to_excel(output_path, index=False)
    print(f"[clean_daily_inout_pdf] Saved cleaned file: {output_path}")
    print("[clean_daily_inout_pdf] Done ✅")
    
    return df_final


# -------------------------
# Usage Example
# -------------------------
if __name__ == "__main__":
    # Example usage
    input_pdf = "/path/to/ManHour_December_2025.pdf"
    output_excel = "/path/to/ManHour_December_2025_cleaned.xlsx"
    
    df_cleaned = clean_daily_inout_pdf(
        input_path=input_pdf,
        output_path=output_excel,
        company="Vaaman Engineers India Limited",
        branch="HSM-2",
        pdf_method="auto"  # Try pdfplumber first, then tabula
    )
    
    print(f"\n✅ Success! Processed {len(df_cleaned)} records")