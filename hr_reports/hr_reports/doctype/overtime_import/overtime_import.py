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
    if not raw:
        return 0.0
    try:
        parts = str(raw).split(":")
        h = int(parts[0]) if len(parts) > 0 else 0
        m = int(parts[1]) if len(parts) > 1 else 0
        # Format as H.MM (e.g., 4.53)
        return float(f"{h}.{m}")
    except Exception:
        return 00.33


class OverTimeImport(Document):
    def validate(self):
        frappe.msgprint(">>> validate() called for OverTimeImport")
        print(">>> validate() called for OverTimeImport")

        if not self.attach_jppy:
            frappe.msgprint("No file attached")
            print("No file attached")
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
        frappe.msgprint(f"Found file at: {file_path}")
        print(f"Found file at: {file_path}")

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

        frappe.msgprint(f"Columns: {list(df.columns)}")
        print(f"Columns: {list(df.columns)}")

        # Step 3: Clear existing rows
        self.set("overtime_import_details", [])
        frappe.msgprint("Cleared old child rows")
        print("Cleared old child rows")

        # Step 4: Append rows
        for _, row in df.iterrows():
            raw_ot = row.get("over_time") or row.get("overtime") or row.get("ot")
            row_data = {
                "employee": row.get("employee") or row.get("employee_id") or row.get("emp"),
                "attendance_date": row.get("attendance_date") or row.get("date") or row.get("attendance_date"),
                "over_time": parse_overtime(raw_ot),  # <-- parsed here
                "shift": row.get("shift"),
            }
            frappe.msgprint(f"Appending row: {row_data}")
            print(f"Appending row: {row_data}")

            self.append("overtime_import_details", row_data)

        frappe.msgprint(f"Successfully imported {len(df)} rows")
        print(f"Successfully imported {len(df)} rows")
