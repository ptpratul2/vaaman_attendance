// Copyright (c) 2026, ms and contributors
// For license information, please see license.txt

frappe.ui.form.on("OT Adjustment", {
	refresh(frm) {
		calculate_total_ot(frm);
	}
});

frappe.ui.form.on("ot adjustment item", {
	additinal_ot(frm) {
		calculate_total_ot(frm);
	},
	ot_adjustment_item_remove(frm) {
		calculate_total_ot(frm);
	}
});

function calculate_total_ot(frm) {
	let total = (frm.doc.ot_adjustment_item || []).reduce((sum, row) => {
		return sum + (row.additinal_ot || 0);
	}, 0);
	frm.set_value("total_ot_hrs", total);
}
