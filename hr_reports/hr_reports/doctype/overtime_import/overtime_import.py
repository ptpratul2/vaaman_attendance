# overtime_import.py
from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.utils.file_manager import get_file_path
import pandas as pd


# -------------------------
# Helper function to parse overtime
# -------------------------
def parse_overtime(raw):
    """
    Convert overtime value to float with 2 decimal places.
    Handles formats like:
    - 4.30 (4 hrs 30 min) → stored as 4.30
    - 4.5 (4.5 hrs) → stored as 4.50
    - 4 → stored as 4.00
    """
    if not raw:
        return 0.0
    try:
        # Convert to float and round to 2 decimal places
        return round(float(raw), 2)
    except (ValueError, TypeError):
        # If conversion fails, return 0.0
        return 0.0


class OverTimeImport(Document):
    def validate(self):
        if not self.attach_jppy:
            frappe.msgprint("No file attached")
            return

        # Step 1: Locate attached file
        file_doc = frappe.get_all(
            "File",
            filters={"file_url": self.attach_jppy},
            fields=["file_url"],
            limit=1
        )
        if not file_doc:
            frappe.throw(f"File not found: {self.attach_jppy}")

        file_path = get_file_path(file_doc[0].file_url)

        # Step 2: Read with pandas (csv/xls/xlsx)
        try:
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
        except Exception as e:
            frappe.throw(f"Failed to read file: {str(e)}")

        # Normalize column names - strip spaces, lowercase, replace spaces and hyphens with underscores
        # Remove parentheses and everything inside them
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"\([^)]*\)", "", regex=True)  # Remove (text)
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.replace("/", "_")
            .str.strip("_")  # Remove leading/trailing underscores
        )
        df = df.where(pd.notnull(df), None)

        # Debug: show what columns were found
        frappe.msgprint(f"Found columns: {', '.join(df.columns.tolist())}")

        # Step 3: Clear existing rows
        self.set("overtime_import_details", [])

        # Step 4: Append rows
        for _, row in df.iterrows():
            raw_ot = row.get("over_time") or row.get("overtime") or row.get("ot")

            # Helper to safely get values and handle NaN
            def safe_get(*args):
                """Get first non-null, non-NaN value from arguments"""
                for val in args:
                    if val is not None and not pd.isna(val):
                        return str(val).strip() if val else None
                return None

            employee_val = safe_get(
                row.get("employee"),
                row.get("employee_id"),
                row.get("emp")
            )
            device_id_val = safe_get(
                row.get("attendance_device_id"),
                row.get("device_id")
            )

            row_data = {
                "employee": employee_val,
                "attendance_device_id_biometricrf_tag_id": device_id_val,
                "attendance_date": safe_get(row.get("attendance_date"), row.get("date")),
                "over_time": parse_overtime(raw_ot),
                "shift": safe_get(row.get("shift")),
            }
            self.append("overtime_import_details", row_data)

        # Step 5: Validate that each row has either employee or device_id
        for idx, row in enumerate(self.overtime_import_details, start=1):
            if not row.employee and not row.attendance_device_id_biometricrf_tag_id:
                frappe.throw(
                    f"Row {idx}: Either Employee or Attendance Device ID must be provided. "
                    f"Found - Employee: '{row.employee}', Device ID: '{row.attendance_device_id_biometricrf_tag_id}'"
                )

        frappe.msgprint(f"Successfully imported {len(df)} rows")
