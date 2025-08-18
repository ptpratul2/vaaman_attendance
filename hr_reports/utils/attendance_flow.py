#attendance_flow.py
import frappe
import os
from hr_reports.utils.clean_crystal_excel import clean_crystal_excel
from frappe.core.doctype.data_import.data_import import start_import

def process_uploaded_file(doc, method):
    """Triggered when Crystal Attendance Upload is submitted"""

    # ------------------------
    # Step 1: Get uploaded file
    # ------------------------
    if not doc.crystal_format:
        frappe.throw("No file found in Crystal Format field.")

    file_doc = frappe.get_doc("File", {"file_url": doc.crystal_format})
    file_name = os.path.basename(file_doc.file_url)
    local_path = frappe.get_site_path("private", "files", file_name)

    frappe.logger().info(f"[Attendance Flow] Step 1: Found raw Crystal file at {local_path}")
    frappe.msgprint(f"Step 1: Found raw file at {local_path}")

    # ------------------------
    # Step 2: Clean the file
    # ------------------------
    cleaned_dir = frappe.get_site_path("private", "files", "cleaned_reports")
    os.makedirs(cleaned_dir, exist_ok=True)
    cleaned_path = os.path.join(cleaned_dir, f"cleaned_{os.path.splitext(file_name)[0]}.xlsx")

    frappe.logger().info("[Attendance Flow] Step 2: Running clean_crystal_excel...")

    # Pass company and branch from the Crystal Attendance Upload doc
    clean_crystal_excel(
        input_path=local_path,
        output_path=cleaned_path,
        company=doc.company,
        branch=doc.branch
    )

    frappe.logger().info(f"[Attendance Flow] Cleaned file saved at {cleaned_path}")
    frappe.msgprint(f"Step 2: Cleaned file saved at {cleaned_path}")

    # ------------------------
    # Step 3: Create Data Import doc
    # ------------------------
    frappe.logger().info("[Attendance Flow] Step 3: Creating Data Import document...")

    # Create a File record for cleaned file (needed by Data Import)
    with open(cleaned_path, "rb") as f:
        file_data = f.read()
    cleaned_file_doc = frappe.get_doc({
        "doctype": "File",
        "file_name": os.path.basename(cleaned_path),
        "is_private": 1,
        "content": file_data
    })
    cleaned_file_doc.save(ignore_permissions=True)

    data_import = frappe.get_doc({
        "doctype": "Data Import",
        "import_type": "Insert New Records",
        "reference_doctype": "Attendance",
        "import_file": cleaned_file_doc.file_url,
        "submit_after_import": 0,   # keep Attendance as draft or 1 to submit automatically
        "mute_emails": 1
    })
    data_import.save(ignore_permissions=True)

    frappe.logger().info(f"[Attendance Flow] Data Import document {data_import.name} created with file {cleaned_file_doc.file_url}")
    frappe.msgprint(f"Step 3: Data Import {data_import.name} created")

    # ------------------------
    # Step 4: Trigger the Import
    # ------------------------
    frappe.logger().info(f"[Attendance Flow] Step 4: Starting background import for {data_import.name}...")
    start_import(data_import.name)

    frappe.msgprint(f"Step 4: Import started for {data_import.name}. Check Import Log for details.")

    # ------------------------
    # Done
    # ------------------------
    frappe.logger().info("[Attendance Flow] ✅ Process complete: Upload → Clean → Auto Import triggered")
