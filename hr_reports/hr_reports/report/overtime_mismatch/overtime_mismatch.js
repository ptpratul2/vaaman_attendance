// overtime_mismatch.js
frappe.query_reports["Overtime Mismatch"] = {
    "filters": [
        {
            "fieldname": "overtime_import",
            "label": "OverTime Import",
            "fieldtype": "Link",
            "options": "OverTime Import",
            "reqd": 1
        }
    ],
    "formatter": function(value, row, column, data, default_formatter) {
        let formatted = default_formatter(value, row, column, data);
        try {
            if (data && data._mismatch_fields && data._mismatch_fields.indexOf(column.fieldname) !== -1) {
                return `<span style="color:red">${formatted}</span>`;
            }
        } catch (e) {}
        return formatted;
    }
};
