# attendance_flow.py
import frappe
import os
from hr_reports.utils.clean_format.clean_crystal_excel import clean_crystal_excel
from hr_reports.utils.clean_format.clean_daily_inout24 import clean_daily_inout24
from hr_reports.utils.clean_format.clean_daily_inout14 import clean_daily_inout14
from hr_reports.utils.clean_format.clean_daily_inout4 import clean_daily_inout4
from hr_reports.utils.clean_format.clean_daily_inout13 import clean_daily_inout13
from hr_reports.utils.clean_format.clean_daily_inout11 import clean_daily_inout11
from hr_reports.utils.clean_format.clean_daily_inout10 import clean_daily_inout10
from hr_reports.utils.clean_format.clean_daily_inout29 import clean_daily_inout29
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
        hzl = ["HZL SK MILL","HZL RD PASTEFILL","HZL Debari O&M","HZL Debari MH","HZL Zawar Stores","HZL SKM Shaft","HZL SKM MH","HZL SKM Conveyor","HZL RDM MH","HZL Ram MH","HZL Pyro O&M","HZL Kayad MH","HZL Debari MCTP","HZL Dariba MH","HZL Chanderia MH","HZL Silver Pantnagar","HZL Pantnagar","HZL Haridwar"]

        # Choose cleaning function based on Branch
        if doc.branch in ["Vedanta Jharsuguda P2","Vedanta Jharsuguda P1"]:
            clean_daily_inout14(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout14 for Vedanta Plant II")

        elif doc.branch == "Vedanta Lanjigarh":
            clean_daily_inout24(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout24 for Lanjigarh")

            # doc.branch == "HZL Pantnagar" or doc.branch == "HZL Debari O&M" :
        elif doc.branch in hzl:
            clean_daily_inout4(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout4 for Rudrapur")
            
        elif doc.branch in ["DOLVI","JSW DOLVI","JSW Dolvi BF"]:
            clean_daily_inout13(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout13 for DOLVI")

        elif doc.branch == "Kakinada":
            clean_daily_inout11(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout11 for Kakinada")

        elif doc.branch in ["Balco", "Balco CH"]:
            clean_daily_inout10(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout10 for Balco")

        elif doc.branch in ["PARADIP", "JSW Paradeep"]:
            clean_daily_inout29(
                input_path=local_path,
                output_path=cleaned_path,
                company=doc.company,
                branch=doc.branch
            )
            append_log(doc, "Step 2: Used clean_daily_inout29 for PARADIP")

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
            "submit_after_import": 1,   # set to 1 if you want Attendance auto-submitted
            "mute_emails": 1
        })
        data_import.save(ignore_permissions=True)
        data_import.db_set("custom_crystal_upload_ref", doc.name, update_modified=False)

        frappe.db.commit()

        append_log(doc, f"Step 3: Data Import {data_import.name} created and linked")

        # ------------------------
        # Step 4: Trigger the Import
        # ------------------------
        try:
            frappe.flags.current_crystal_upload = doc.name

            start_import(data_import.name)
            append_log(doc, f"Step 4: Import started for {data_import.name}. Check Import Log for details.")
        except Exception as e:
            append_log(doc, f"❌ Import failed: {str(e)}")
            raise
        finally:
            frappe.flags.current_crystal_upload = None  # always clear


        # ------------------------
        # Done
        # ------------------------
        append_log(doc, "✅ Process complete: Upload → Clean → Auto Import triggered")

    except Exception as e:
        append_log(doc, f"❌ ERROR: {str(e)}")
        raise


def cancel_uploaded_file(doc, method):
    """Triggered when Crystal Attendance Upload is cancelled"""
    try:
        append_log(doc, "Cancel triggered → rolling back imported attendance")

        # 1. Delete Attendance linked to this upload
        attendances = frappe.get_all(
            "Attendance",
            filters={"custom_crystal_upload_ref": doc.name},
            pluck="name"
        )
        for att in attendances:
            try:
                att_doc = frappe.get_doc("Attendance", att)
                att_doc.cancel() if att_doc.docstatus == 1 else att_doc.delete()
                append_log(doc, f"Removed Attendance {att}")
            except Exception as e:
                append_log(doc, f"❌ Failed to remove Attendance {att}: {str(e)}")

        # 2. Delete Data Import if exists
        data_imports = frappe.get_all(
            "Data Import",
            filters={"import_file": ["like", f"%cleaned_{doc.name}%"]},
            pluck="name"
        )
        for di in data_imports:
            try:
                frappe.delete_doc("Data Import", di, force=1)
                append_log(doc, f"Removed Data Import {di}")
            except Exception as e:
                append_log(doc, f"❌ Failed to remove Data Import {di}: {str(e)}")

        # 3. Optionally remove cleaned report file
        cleaned_dir = frappe.get_site_path("private", "files", "cleaned_reports")
        for f in os.listdir(cleaned_dir):
            if f"cleaned_{doc.name}" in f:
                try:
                    os.remove(os.path.join(cleaned_dir, f))
                    append_log(doc, f"Removed cleaned file {f}")
                except Exception:
                    pass

        append_log(doc, "✅ Cancel complete: Attendance + imports removed")

    except Exception as e:
        append_log(doc, f"❌ Cancel error: {str(e)}")
        raise

def after_insert_attendance(doc, method):
    """Automatically stamp Attendance with current Crystal Upload ref if available"""
    if frappe.flags.current_crystal_upload:
        frappe.logger().debug(f"[DEBUG] Stamping {doc.name} with {frappe.flags.current_crystal_upload}")
        doc.db_set("custom_crystal_upload_ref", frappe.flags.current_crystal_upload, update_modified=False)
    else:
        frappe.logger().debug(f"[DEBUG] No crystal_upload_ref set for {doc.name}")
