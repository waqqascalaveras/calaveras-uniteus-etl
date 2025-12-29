/**
 * Utility Functions for Dashboard
 * 
 * This module contains common utility functions used across the dashboard,
 * including chart helpers, data validation, formatting, and UI utilities.
 */

// Global charts storage - use window object to ensure single instance
window.dashboardCharts = window.dashboardCharts || {};
export const charts = window.dashboardCharts;

// Color schemes for charts
export const colorSchemes = {
    primary: ['#2c5282', '#3182ce', '#4299e1', '#63b3ed', '#90cdf4', '#bee3f8'],
    success: ['#22543d', '#276749', '#2f855a', '#38a169', '#48bb78', '#68d391'],
    warm: ['#c05621', '#dd6b20', '#ed8936', '#f6ad55', '#fbd38d', '#feebc8'],
    diverse: ['#2c5282', '#38a169', '#dd6b20', '#805ad5', '#d53f8c', '#319795', '#e53e3e', '#f6ad55']
};

/**
 * Safely create a Chart.js chart with proper error handling
 * @param {string} canvasId - The ID of the canvas element
 * @param {string} chartName - The name/key to store the chart under
 * @param {object} chartConfig - Chart.js configuration object
 * @returns {Promise<Chart|null>} The created chart or null
 */
export function createChartSafely(canvasId, chartName, chartConfig) {
    return new Promise((resolve, reject) => {
        try {
            const canvas = document.getElementById(canvasId);
            
            // Validate canvas exists
            if (!canvas) {
                console.error(`[createChartSafely] Canvas element '${canvasId}' not found in DOM`);
                reject(new Error(`Canvas ${canvasId} not found`));
                return;
            }
            
            // Validate canvas has dimensions (critical for Chart.js)
            const rect = canvas.getBoundingClientRect();
            if (rect.width === 0 || rect.height === 0) {
                console.warn(`[createChartSafely] Canvas '${canvasId}' has zero dimensions (${rect.width}x${rect.height}). Parent may be hidden.`);
                // Don't reject - canvas might be in hidden tab, which is OK
                resolve(null);
                return;
            }
            
            // Sanitize chart data before validation
            if (chartConfig.data && chartConfig.data.datasets) {
                chartConfig.data.datasets.forEach(dataset => {
                    if (dataset && dataset.data) {
                        dataset.data = sanitizeChartData(dataset.data);
                    }
                });
            }
            
            // Validate chart data
            if (chartConfig.data && chartConfig.data.datasets) {
                const dataset = chartConfig.data.datasets[0];
                if (dataset && dataset.data) {
                    const hasValidData = dataset.data.some(v => v != null && v > 0);
                    if (!hasValidData) {
                        console.warn(`[createChartSafely] Chart '${chartName}' has no valid data to display`);
                    }
                }
            }
            
            // Destroy existing chart if it exists - check both our registry and Chart.js registry
            if (charts[chartName]) {
                try {
                    console.log(`[createChartSafely] Destroying existing chart from registry: ${chartName}`);
                    charts[chartName].destroy();
                    delete charts[chartName];
                } catch (e) {
                    console.warn(`[createChartSafely] Error destroying chart ${chartName}:`, e);
                }
            }
            
            // Also check Chart.js's internal registry for this canvas
            try {
                // Chart.getChart is available in Chart.js 3.0+
                if (typeof Chart !== 'undefined' && Chart.getChart) {
                    const existingChart = Chart.getChart(canvas);
                    if (existingChart) {
                        console.log(`[createChartSafely] Destroying existing chart from Chart.js registry for canvas ${canvasId}`);
                        existingChart.destroy();
                    }
                }
            } catch (e) {
                // Chart.getChart might not exist in older versions, or canvas might not have a chart
                // This is fine, continue
                console.warn(`[createChartSafely] Could not check Chart.js registry:`, e);
            }
            
            // Use requestAnimationFrame to ensure DOM is fully rendered
            requestAnimationFrame(() => {
                try {
                    console.log(`[createChartSafely] Creating chart: ${chartName} on canvas ${canvasId}`);
                    charts[chartName] = new Chart(canvas, chartConfig);
                    console.log(`[createChartSafely] ✓ Chart ${chartName} created successfully`);
                    resolve(charts[chartName]);
                } catch (error) {
                    console.error(`[createChartSafely] ✗ Failed to create chart ${chartName}:`, error);
                    console.error(`[createChartSafely] Canvas dimensions: ${rect.width}x${rect.height}`);
                    console.error(`[createChartSafely] Chart config:`, chartConfig);
                    reject(error);
                }
            });
            
        } catch (error) {
            console.error(`[createChartSafely] Unexpected error for chart ${chartName}:`, error);
            reject(error);
        }
    });
}

/**
 * Sanitize chart data values - convert null, undefined, NaN to 0
 * @param {Array} dataArray - Array of data values
 * @returns {Array} Sanitized array with valid numbers
 */
export function sanitizeChartData(dataArray) {
    if (!Array.isArray(dataArray)) {
        return [];
    }
    
    return dataArray.map(value => {
        // Convert null, undefined, NaN, empty string to 0
        if (value === null || value === undefined || value === '' || isNaN(value)) {
            return 0;
        }
        // Ensure it's a number
        const num = typeof value === 'number' ? value : parseFloat(value);
        return isNaN(num) ? 0 : num;
    });
}

/**
 * Sanitize chart data object (labels and values)
 * @param {object} data - Chart data object with labels and values
 * @returns {object} Sanitized data object
 */
export function sanitizeChartDataObject(data) {
    if (!data || typeof data !== 'object') {
        return { labels: [], values: [] };
    }
    
    const labels = Array.isArray(data.labels) ? data.labels : [];
    const values = Array.isArray(data.values) ? sanitizeChartData(data.values) : [];
    
    // Ensure arrays have the same length
    const maxLength = Math.max(labels.length, values.length);
    const sanitized = [];
    
    for (let i = 0; i < maxLength; i++) {
        const label = i < labels.length ? labels[i] : null;
        const value = i < values.length ? values[i] : 0;
        
        // Sanitize value
        const sanitizedValue = (value === null || value === undefined || isNaN(value)) ? 0 : Number(value);
        
        // Only include items with valid labels or non-zero values
        if (label != null || sanitizedValue > 0) {
            sanitized.push({
                label: label != null ? String(label) : `Item ${i + 1}`,
                value: sanitizedValue
            });
        }
    }
    
    // If all values are zero, keep at least one entry to show "No Data" state
    if (sanitized.length === 0 && labels.length > 0) {
        return {
            labels: labels.slice(0, 1).map(l => l != null ? String(l) : 'No Data'),
            values: [0]
        };
    }
    
    return {
        labels: sanitized.map(item => item.label),
        values: sanitized.map(item => item.value)
    };
}

/**
 * Validate chart data structure before rendering
 * @param {object} data - Chart data object with labels and values
 * @param {string} chartName - Name of the chart for logging
 * @returns {boolean} True if data is valid
 */
export function validateChartData(data, chartName) {
    if (!data) {
        console.error(`[validateChartData] ${chartName}: data is null or undefined`);
        return false;
    }
    
    if (!data.labels || !Array.isArray(data.labels)) {
        console.error(`[validateChartData] ${chartName}: missing or invalid labels array`);
        return false;
    }
    
    if (!data.values || !Array.isArray(data.values)) {
        console.error(`[validateChartData] ${chartName}: missing or invalid values array`);
        return false;
    }
    
    if (data.labels.length !== data.values.length) {
        console.error(`[validateChartData] ${chartName}: labels (${data.labels.length}) and values (${data.values.length}) length mismatch`);
        return false;
    }
    
    if (data.labels.length === 0) {
        console.warn(`[validateChartData] ${chartName}: empty data arrays`);
        return false;
    }
    
    // Check for non-numeric values (after sanitization, should only be numbers)
    const invalidValues = data.values.filter(v => typeof v !== 'number' && v !== null);
    if (invalidValues.length > 0) {
        console.error(`[validateChartData] ${chartName}: non-numeric values found:`, invalidValues);
        return false;
    }
    
    console.log(`[validateChartData] ✓ ${chartName}: data is valid (${data.labels.length} items)`);
    return true;
}

/**
 * Create a safe datalabels formatter with comprehensive null checks
 * @returns {function} Formatter function for Chart.js datalabels plugin
 */
export function createDatalabelsFormatter() {
    return function(value, context) {
        try {
            // Validate context - be extra defensive
            if (!context) return '';
            if (!context.chart) return '';
            if (!context.chart.data) return '';
            if (!context.chart.data.datasets) return '';
            if (!context.chart.data.datasets[0]) return '';
            
            const dataset = context.chart.data.datasets[0];
            if (!dataset.data || !Array.isArray(dataset.data)) return '';
            
            // Sanitize value
            const sanitizedValue = (value === null || value === undefined || isNaN(value)) ? 0 : Number(value);
            
            // Sanitize array and calculate sum
            const arr = dataset.data.map(v => {
                const num = (v === null || v === undefined || isNaN(v)) ? 0 : Number(v);
                return isNaN(num) ? 0 : num;
            });
            const sum = arr.reduce((a, b) => (a || 0) + (b || 0), 0) || 0;
            
            // Don't display labels for zero or null values
            if (sanitizedValue === 0 || sum === 0) {
                return '';
            }
            
            // Validate we can calculate percentage
            if (typeof sanitizedValue !== 'number' || typeof sum !== 'number' || sum === 0) return '';
            
            const percentage = ((sanitizedValue / sum) * 100).toFixed(1) + '%';
            const count = sanitizedValue.toLocaleString();
            
            // Return two-line label: percentage and count
            return [percentage, `n=${count}`];
        } catch (error) {
            // Silently fail - don't log to avoid console spam
            return '';
        }
    };
}

/**
 * Create safe datalabels configuration that handles null values
 * @param {object} options - Additional datalabels options
 * @returns {object} Datalabels plugin configuration
 */
export function createSafeDatalabelsConfig(options = {}) {
    return {
        display: true,
        formatter: createDatalabelsFormatter(),
        color: '#fff',
        font: {
            weight: 'bold',
            size: 11
        },
        // Add positioner that handles null contexts
        positioner: function(elements) {
            if (!elements || elements.length === 0) return false;
            const element = elements[0];
            if (!element || !element.element || !element.element.x || !element.element.y) {
                return false;
            }
            return {
                x: element.element.x,
                y: element.element.y
            };
        },
        ...options
    };
}

/**
 * Display "No data available" message on canvas
 * @param {string} canvasId - Canvas element ID
 * @param {string} message - Message to display
 */
export function displayNoDataMessage(canvasId, message) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    const width = canvas.width;
    const height = canvas.height;
    
    // Clear canvas
    ctx.clearRect(0, 0, width, height);
    
    // Set text style
    ctx.fillStyle = '#6c757d';
    ctx.font = '16px Arial';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    
    // Draw message
    ctx.fillText(message || 'No data available', width / 2, height / 2);
}

/**
 * Safely format numbers with fallback
 * @param {*} value - Value to format
 * @param {number} decimals - Number of decimal places
 * @param {string} fallback - Fallback string if invalid
 * @returns {string} Formatted number or fallback
 */
export function safeNumber(value, decimals = 0, fallback = 'N/A') {
    if (value === null || value === undefined || value === '' || isNaN(value)) {
        return fallback;
    }
    if (typeof value === 'number') {
        return decimals > 0 ? value.toFixed(decimals) : value.toString();
    }
    const num = parseFloat(value);
    return isNaN(num) ? fallback : (decimals > 0 ? num.toFixed(decimals) : num.toString());
}

/**
 * Safely format percentages
 * @param {*} value - Value to format as percentage
 * @param {number} decimals - Number of decimal places
 * @param {string} fallback - Fallback string if invalid
 * @returns {string} Formatted percentage or fallback
 */
export function safePercent(value, decimals = 1, fallback = '0.0%') {
    const num = safeNumber(value, decimals, null);
    return num === null ? fallback : `${num}%`;
}

/**
 * Check if data is valid (not null, not error object)
 * @param {*} data - Data to validate
 * @returns {boolean} True if data is valid
 */
export function hasValidData(data) {
    return data && 
           typeof data === 'object' && 
           !data.error;
}

/**
 * Escape HTML to prevent XSS
 * @param {string} text - Text to escape
 * @returns {string} Escaped HTML
 */
export function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Display loading state in table
 * @param {HTMLElement} tbody - Table body element
 * @param {number} colspan - Number of columns
 * @param {string} message - Loading message
 */
export function showTableLoading(tbody, colspan, message = 'Loading...') {
    tbody.innerHTML = `<tr><td colspan="${colspan}" class="text-center text-muted py-3"><i class="fas fa-spinner fa-spin me-2"></i>${message}</td></tr>`;
}

/**
 * Display empty state in table
 * @param {HTMLElement} tbody - Table body element
 * @param {number} colspan - Number of columns
 * @param {string} message - Empty message
 */
export function showTableEmpty(tbody, colspan, message = 'No data available') {
    tbody.innerHTML = `<tr><td colspan="${colspan}" class="text-center text-muted py-3"><i class="fas fa-info-circle me-2"></i>${message}</td></tr>`;
}

/**
 * Display error state in table
 * @param {HTMLElement} tbody - Table body element
 * @param {number} colspan - Number of columns
 * @param {*} error - Error object or message
 */
export function showTableError(tbody, colspan, error) {
    const message = typeof error === 'string' ? error : error.message || 'Unknown error';
    tbody.innerHTML = `<tr><td colspan="${colspan}" class="text-center text-danger py-3"><i class="fas fa-exclamation-triangle me-2"></i>Error: ${escapeHtml(message)}</td></tr>`;
}

/**
 * Export chart as PNG image
 * @param {string} chartId - Canvas element ID
 * @param {string} filename - Base filename for download
 */
export function exportChart(chartId, filename) {
    const canvas = document.getElementById(chartId);
    const url = canvas.toDataURL('image/png');
    const link = document.createElement('a');
    link.download = `${filename}-${new Date().toISOString().split('T')[0]}.png`;
    link.href = url;
    link.click();
}

/**
 * Export table to CSV file
 * @param {string} tableId - Table element ID
 * @param {string} filename - Base filename for download
 */
export function exportTableToCSV(tableId, filename) {
    const table = document.getElementById(tableId);
    let csv = [];
    
    const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent.trim());
    csv.push(headers.join(','));
    
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const cols = Array.from(row.querySelectorAll('td')).map(td => `"${td.textContent.trim()}"`);
        if (cols.length > 0 && cols[0] !== '"Loading..."') csv.push(cols.join(','));
    });
    
    const blob = new Blob([csv.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.download = `${filename}-${new Date().toISOString().split('T')[0]}.csv`;
    link.href = url;
    link.click();
}

/**
 * Show toast notification
 * @param {string} message - Message to display
 * @param {string} type - Toast type: 'success', 'info', 'danger'
 */
export function showToast(message, type) {
    const bgClass = type === 'success' ? 'bg-success' : type === 'info' ? 'bg-info' : 'bg-danger';
    const toast = document.createElement('div');
    toast.className = `toast align-items-center text-white ${bgClass} border-0 position-fixed top-0 end-0 m-3`;
    toast.setAttribute('role', 'alert');
    toast.style.zIndex = '9999';
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">${message}</div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>
    `;
    document.body.appendChild(toast);
    const bsToast = new bootstrap.Toast(toast);
    bsToast.show();
    setTimeout(() => toast.remove(), 5000);
}

/**
 * Show success toast notification
 * @param {string} message - Success message
 */
export function showSuccess(message) {
    showToast(message, 'success');
}

/**
 * Show info toast notification
 * @param {string} message - Info message
 */
export function showInfo(message) {
    showToast(message, 'info');
}

/**
 * Show error toast notification
 * @param {string} message - Error message
 */
export function showError(message) {
    showToast(message, 'danger');
}
