/**
 * Export Utilities for CSV and JSON
 */

export function exportToCSV(data, filename = 'kemet-data.csv') {
    if (!data || data.length === 0) {
        alert('No data to export');
        return;
    }

    // Get column headers from first row
    const headers = Object.keys(data[0]);

    // Create CSV rows
    const csvRows = [
        headers.join(','), // Header row
        ...data.map(row =>
            headers.map(header => {
                const value = row[header];
                // Escape quotes and wrap in quotes if contains comma or quote
                const stringValue = value === null || value === undefined ? '' : String(value);
                return stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')
                    ? `"${stringValue.replace(/"/g, '""')}"` // Escape quotes
                    : stringValue;
            }).join(',')
        )
    ];

    const csvContent = csvRows.join('\n');
    downloadFile(csvContent, filename, 'text/csv;charset=utf-8;');
}

export function exportToJSON(data, filename = 'kemet-data.json') {
    if (!data || data.length === 0) {
        alert('No data to export');
        return;
    }

    const jsonContent = JSON.stringify(data, null, 2);
    downloadFile(jsonContent, filename, 'application/json;charset=utf-8;');
}

function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);

    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.style.display = 'none';

    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    // Clean up
    setTimeout(() => URL.revokeObjectURL(url), 100);
}

export default {
    exportToCSV,
    exportToJSON
};
