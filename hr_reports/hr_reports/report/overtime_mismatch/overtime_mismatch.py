# overtime_mismatch.py
from __future__ import unicode_literals
import frappe


def _to_float(v):
   try:
       return float(v)
   except:
       return 0.0


def execute(filters=None):
    frappe.clear_cache()
    columns = [
       {"fieldname": "branch", "label": "Branch", "fieldtype": "Data"},
       {"fieldname": "employee", "label": "Employee", "fieldtype": "Data"},
       {"fieldname": "device_id", "label": "Attendance Device ID", "fieldtype": "Data"},
       {"fieldname": "attendance_date", "label": "Date", "fieldtype": "Date"},
       {"fieldname": "import_overtime", "label": "Imported OT", "fieldtype": "Float", "precision": 2},
       {"fieldname": "system_overtime", "label": "System OT", "fieldtype": "Float", "precision": 2},
       {"fieldname": "shift", "label": "Shift", "fieldtype": "Data"},
       {"fieldname": "mismatch", "label": "Mismatch", "fieldtype": "Data"},
    ]

    data = []

    if not filters or not filters.get("overtime_import"):
        frappe.msgprint("Please select an Overtime Import to view mismatches.")
        return columns, data
    oi_name = filters.get("overtime_import")
    doc = frappe.get_doc("OverTime Import", oi_name)

    for row in doc.overtime_import_details:
       imported_ot = _to_float(row.over_time)
       system_ot = 0.0

       # Determine employee: either from direct employee field or by finding via device_id
       employee_id = row.employee
       if not employee_id and row.attendance_device_id_biometricrf_tag_id:
           # Find employee by device_id
           employee_id = frappe.db.get_value(
               "Employee",
               {"attendance_device_id": row.attendance_device_id_biometricrf_tag_id},
               "name",
           )

       attendance_name = None
       if employee_id:
           attendance_name = frappe.db.get_value(
               "Attendance",
               {"employee": employee_id, "attendance_date": row.attendance_date},
               "name",
           )

       att = None
       if attendance_name:
           att = frappe.get_doc("Attendance", attendance_name)
           # try likely field names on Attendance that may hold overtime value
           for field in (
               "custom_over_time",
               "over_time",
               "overtime_hours",
               "overtime_hours_in_seconds",
           ):
               if hasattr(att, field):
                   val = getattr(att, field)
                   system_ot = _to_float(val)
                   break

       # Round both values to 2 decimal places for comparison
       imported_ot_rounded = round(imported_ot, 2)
       system_ot_rounded = round(system_ot, 2)
       mismatch = abs(imported_ot_rounded - system_ot_rounded) > 0.01

       rec = {
           "branch": (row.branch or doc.branch or (att.custom_branch if att and getattr(att, "custom_branch", None) else "") or ""),
           "employee": employee_id or "Unknown",
           "device_id": row.attendance_device_id_biometricrf_tag_id or "",
           "attendance_date": row.attendance_date,
           "import_overtime": imported_ot_rounded,
           "system_overtime": system_ot_rounded,
           "shift": row.shift or (att.shift if att else "") or "",
           "mismatch": "Yes" if mismatch else "No",
       }       
       # add a small helper so the frontend formatter can highlight columns
       if mismatch and att:
           rec["_mismatch_fields"] = ["import_overtime", "system_overtime"]
       data.append(rec)

    if not data:
       data.append(
           {
               "branch": "",
               "employee": "",
               "device_id": "",
               "attendance_date": frappe.utils.nowdate(),
               "import_overtime": 0.0,
               "system_overtime": 0.0,
               "shift": "",
               "mismatch": "No",
           }
       )

    return columns, data

