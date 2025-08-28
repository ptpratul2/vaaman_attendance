# attendance_flow.py
import frappe
import os
from hr_reports.utils.clean_format.clean_crystal_excel import clean_crystal_excel
from hr_reports.utils.clean_format.clean_daily_inout24 import clean_daily_inout24
from hr_reports.utils.clean_format.clean_daily_inout14 import clean_daily_inout14
from frappe.core.doctype.data_import.data_import import start_import


def append_log(doc, message):
    """Append log line to processing_log field with timestamp"""
    new_log = (doc.processing_log or "") + f"\n{frappe.utils.now()} - {message}"
    doc.db_set("processing_log", new_log, update_modified=False)


def process_uploaded_file(doc, method):
    """Triggered when Crystal Attendance Upload is submitted"""
    try:
        # ------------------------
        # Step 1: Get uploaded file
        # ------------------------
        if not doc.crystal_format:
            frappe.throw("No file found in Crystal Format field.")

        file_doc = frappe.get_doc("File", {"file_url": doc.crystal_format})
        file_name = os.path.basename(file_doc.file_url)
        local_path = frappe.get_site_path("private", "files", file_name)

        append_log(doc, f"Step 1: Found raw file at {local_path}")

        # ------------------------
        # Step 2: Clean the file
        # ------------------------
        cleaned_dir = frappe.get_site_path("private", "files", "cleaned_reports")
        os.makedirs(cleaned_dir, exist_ok=True)
        cleaned_path = os.path.join(cleaned_dir, f"cleaned_{os.path.splitext(file_name)[0]}.xlsx")

        # Choose cleaning function based on Branch
        if doc.branch == "VEDANTA PLANT II":
            clean_daily_inout14(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout14 for Vedanta Plant II")
        elif doc.branch == "Lanjigarh":
            clean_daily_inout24(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout24 for Lanjigarh")
        else:
            clean_crystal_excel(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_crystal_excel for default format")


        append_log(doc, f"Step 2: Cleaned file saved at {cleaned_path}")

        # ------------------------
        # Step 3: Create Data Import doc
        # ------------------------
        with open(cleaned_path, "rb") as f:
            file_data = f.read()
        cleaned_file_doc = frappe.get_doc({
            "doctype": "File",
            "file_name": os.path.basename(cleaned_path),
            "is_private": 1,
            "content": file_data
        })
        cleaned_file_doc.save(ignore_permissions=True)
        frappe.db.commit()

        data_import = frappe.get_doc({
            "doctype": "Data Import",
            "import_type": "Insert New Records",
            "reference_doctype": "Attendance",
            "import_file": cleaned_file_doc.file_url,
            "submit_after_import": 0,   # set to 1 if you want Attendance auto-submitted
            "mute_emails": 1
        })
        data_import.save(ignore_permissions=True)
        frappe.db.commit()

        append_log(doc, f"Step 3: Data Import {data_import.name} created")

        # ------------------------
        # Step 4: Trigger the Import
        # ------------------------
        try:
            start_import(data_import.name)
            append_log(doc, f"Step 4: Import started for {data_import.name}. Check Import Log for details.")
        except Exception as e:
            append_log(doc, f"❌ Import failed: {str(e)}")
            raise

        # ------------------------
        # Done
        # ------------------------
        append_log(doc, "✅ Process complete: Upload → Clean → Auto Import triggered")

    except Exception as e:
        append_log(doc, f"❌ ERROR: {str(e)}")
        raise
