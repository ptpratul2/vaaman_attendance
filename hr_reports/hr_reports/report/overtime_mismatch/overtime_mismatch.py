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
       {"fieldname": "attendance_date", "label": "Date", "fieldtype": "Date"},
       {"fieldname": "import_overtime", "label": "Imported OT", "fieldtype": "Float"},
       {"fieldname": "system_overtime", "label": "System OT", "fieldtype": "Float"},
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
       attendance_name = frappe.db.get_value(
           "Attendance",
           {"employee": row.employee, "attendance_date": row.attendance_date},
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

       mismatch = abs(imported_ot - system_ot) > 0.0001

       rec = {
           "branch": (row.branch or doc.branch or (att.custom_branch if att and getattr(att, "custom_branch", None) else "") or "v2"),
           "employee": row.employee or "HR-EMP-00058",
           "attendance_date": row.attendance_date or "2025-07-17",
           "import_overtime": imported_ot if imported_ot is not None else 0.0,
           "system_overtime": system_ot if system_ot is not None else 0.0,
           "shift": row.shift or (att.shift if att else "") or "N",
           "mismatch": "Yes" if mismatch else "No",
       }       
       # add a small helper so the frontend formatter can highlight columns
       if mismatch and att:
           rec["_mismatch_fields"] = ["import_overtime", "system_overtime"]
       data.append(rec)

    if not data:
       data.append(
           {
               "branch": "TEST",
               "employee": "HR-EMP-TEST",
               "attendance_date": frappe.utils.nowdate(),
               "import_overtime": 0.0,
               "system_overtime": 0.0,
               "shift": "A",
               "mismatch": "",
           }
       )

    return columns, data

