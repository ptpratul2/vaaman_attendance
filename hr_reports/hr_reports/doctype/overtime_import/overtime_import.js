// Copyright (c) 2025, ms and contributors
// For license information, please see license.txt

frappe.ui.form.on("OverTime Import", {
	refresh(frm) {
		// Set custom formatter for over_time field in child table
		frm.fields_dict.overtime_import_details.grid.update_docfield_property(
			'over_time',
			'formatter',
			function(value, df, options, doc) {
				if (value != null && value !== '') {
					// Format to 2 decimal places
					return parseFloat(value).toFixed(2);
				}
				return value;
			}
		);

		// Refresh the grid to apply formatter
		frm.fields_dict.overtime_import_details.grid.refresh();
	}
});
