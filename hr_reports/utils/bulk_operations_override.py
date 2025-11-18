"""
Override Frappe's bulk operation limits for edit and delete operations.
This module extends the default 500 record limit based on HR Reports Settings.
"""

import frappe
from frappe import _


def get_bulk_operation_limit():
	"""
	Get the maximum bulk operation limit from HR Reports Settings.
	Returns the configured limit if enabled, otherwise returns default 500.
	"""
	try:
		# Check if settings exist
		if not frappe.db.exists("DocType", "HR Reports Settings"):
			return 500
		
		settings = frappe.get_cached_doc("HR Reports Settings")
		if settings.get("enable_extended_bulk_operations"):
			limit = settings.get("max_bulk_operation_limit") or 10000
			# Ensure minimum is 500 and maximum is reasonable (100000)
			return max(500, min(int(limit), 100000))
	except Exception:
		# If settings don't exist or error, return default
		pass
	return 500


@frappe.whitelist()
def submit_cancel_or_update_docs(doctype, docnames, action="submit", data=None, task_id=None):
	"""
	Override Frappe's submit_cancel_or_update_docs to support extended limits.
	This is called when bulk operations are performed from the UI.
	"""
	from frappe.desk.doctype.bulk_update.bulk_update import _bulk_action
	
	if isinstance(docnames, str):
		docnames = frappe.parse_json(docnames)

	max_limit = get_bulk_operation_limit()
	
	if len(docnames) < 20:
		return _bulk_action(doctype, docnames, action, data, task_id)
	elif len(docnames) <= max_limit:
		frappe.msgprint(_("Bulk operation is enqueued in background."), alert=True)
		frappe.enqueue(
			_bulk_action,
			doctype=doctype,
			docnames=docnames,
			action=action,
			data=data,
			task_id=task_id,
			queue="short",
			timeout=1000,
		)
	else:
		frappe.throw(
			_("Bulk operations only support up to {0} documents.").format(max_limit),
			title=_("Too Many Documents")
		)


@frappe.whitelist()
def delete_items():
	"""
	Override Frappe's delete_items to support extended limits.
	This is called when bulk delete is performed from the UI.
	"""
	import json
	from frappe.desk.reportview import delete_bulk
	
	items = sorted(json.loads(frappe.form_dict.get("items")), reverse=True)
	doctype = frappe.form_dict.get("doctype")
	
	max_limit = get_bulk_operation_limit()
	
	# Check limit
	if len(items) > max_limit:
		frappe.throw(
			_("Bulk delete only supports up to {0} documents.").format(max_limit),
			title=_("Too Many Documents")
		)
	
	if len(items) > 10:
		frappe.enqueue("frappe.desk.reportview.delete_bulk", doctype=doctype, items=items)
	else:
		delete_bulk(doctype, items)


@frappe.whitelist()
def bulk_workflow_approval(docnames, doctype, action):
	"""
	Override Frappe's bulk_workflow_approval to support extended limits.
	"""
	import json
	from frappe.model.workflow import _bulk_workflow_action
	
	docnames = json.loads(docnames)
	max_limit = get_bulk_operation_limit()
	
	if len(docnames) < 20:
		_bulk_workflow_action(docnames, doctype, action)
	elif len(docnames) <= max_limit:
		frappe.msgprint(_("Bulk {0} is enqueued in background.").format(action), alert=True)
		frappe.enqueue(
			_bulk_workflow_action,
			docnames=docnames,
			doctype=doctype,
			action=action,
			queue="short",
			timeout=1000,
		)
	else:
		frappe.throw(
			_("Bulk approval only support up to {0} documents.").format(max_limit),
			title=_("Too Many Documents")
		)


@frappe.whitelist()
def bulk_update(self):
	"""
	Override Frappe's bulk_update to support extended limits.
	This is called from the Bulk Update tool.
	"""
	from frappe.desk.doctype.bulk_update.bulk_update import submit_cancel_or_update_docs
	from frappe.utils import cint
	
	self.check_permission("write")
	max_limit = get_bulk_operation_limit()
	
	# Use the configured limit instead of hardcoded 500
	limit = self.limit if self.limit and cint(self.limit) < max_limit else max_limit

	condition = ""
	if self.condition:
		if ";" in self.condition:
			frappe.throw(_("; not allowed in condition"))
		condition = f" where {self.condition}"

	docnames = frappe.db.sql_list(
		f"""select name from `tab{self.document_type}`{condition} limit {limit} offset 0"""
	)
	return submit_cancel_or_update_docs(
		self.document_type, docnames, "update", {self.field: self.update_value}
	)

