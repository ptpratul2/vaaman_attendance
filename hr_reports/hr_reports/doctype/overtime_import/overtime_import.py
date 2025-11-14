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

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df.where(pd.notnull(df), None)

        # Step 3: Clear existing rows
        self.set("overtime_import_details", [])

        # Step 4: Append rows
        for _, row in df.iterrows():
            raw_ot = row.get("over_time") or row.get("overtime") or row.get("ot")
            row_data = {
                "employee": row.get("employee") or row.get("employee_id") or row.get("emp"),
                "attendance_date": row.get("attendance_date") or row.get("date") or row.get("attendance_date"),
                "over_time": parse_overtime(raw_ot),  # <-- parsed here
                "shift": row.get("shift"),
            }
            self.append("overtime_import_details", row_data)

        frappe.msgprint(f"Successfully imported {len(df)} rows")
