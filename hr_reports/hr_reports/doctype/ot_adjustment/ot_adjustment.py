# Copyright (c) 2026, ms and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.utils.file_manager import get_file_path
import pandas as pd


def parse_overtime(raw):
	if not raw:
		return 0.0
	try:
		return round(float(raw), 2)
	except (ValueError, TypeError):
		return 0.0


class OTAdjustment(Document):
	def validate(self):
		if not self.attach_hlcs:
			frappe.msgprint("No file attached")
			return

		file_doc = frappe.get_all(
			"File",
			filters={"file_url": self.attach_hlcs},
			fields=["file_url"],
			limit=1
		)
		if not file_doc:
			frappe.throw(f"File not found: {self.attach_hlcs}")

		file_path = get_file_path(file_doc[0].file_url)

		try:
			if file_path.endswith(".csv"):
				df = pd.read_csv(file_path)
			else:
				df = pd.read_excel(file_path)
		except Exception as e:
			frappe.throw(f"Failed to read file: {str(e)}")

		df.columns = (
			df.columns.str.strip()
			.str.lower()
			.str.replace(r"\([^)]*\)", "", regex=True)
			.str.replace(" ", "_")
			.str.replace("-", "_")
			.str.replace("/", "_")
			.str.strip("_")
		)
		df = df.where(pd.notnull(df), None)

		frappe.msgprint(f"Found columns: {', '.join(df.columns.tolist())}")

		self.set("ot_adjustment_item", [])

		def safe_get(*args):
			for val in args:
				if val is not None and not pd.isna(val):
					return str(val).strip() if val else None
			return None

		for _, row in df.iterrows():
			raw_ot = None
			for col in ["over_time", "overtime", "ot", "additinal_ot", "additional_ot"]:
				val = row.get(col)
				if val is not None and not pd.isna(val):
					raw_ot = val
					break

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
				"additinal_ot": parse_overtime(raw_ot),
			}
			self.append("ot_adjustment_item", row_data)

		self.total_ot_hrs = sum(row.additinal_ot or 0 for row in self.ot_adjustment_item)

		for idx, row in enumerate(self.ot_adjustment_item, start=1):
			if not row.employee and not row.attendance_device_id_biometricrf_tag_id:
				frappe.throw(
					f"Row {idx}: Either Employee or Attendance Device ID must be provided. "
					f"Found - Employee: '{row.employee}', Device ID: '{row.attendance_device_id_biometricrf_tag_id}'"
				)

		frappe.msgprint(f"Successfully imported {len(df)} rows")
