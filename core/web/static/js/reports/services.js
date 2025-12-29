/**
 * Services Tab Reports
 * 
 * Contains service-related reports: resolution time, conversion rates,
 * service subtypes, and outcomes.
 */

import { showTableLoading, showTableEmpty, showTableError, escapeHtml, safeNumber, safePercent } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

export async function loadServicesReports() {
    if (document.querySelector('#resolutionTimeTable tbody tr td')?.textContent !== 'Loading...') return;
    
    await Promise.all([
        loadResolutionTimeTable(),
        loadConversionTable(),
        loadSubtypesTable(),
        loadOutcomesTable()
    ]);
}

export async function loadResolutionTimeTable() {
    const response = await fetch('/api/reports/service-metrics/resolution-time' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#resolutionTimeTable tbody');
    if (data.metrics.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.metrics.map(m => `
        <tr>
            <td>${escapeHtml(m.service_type)}</td>
            <td class="text-end">${m.total_cases}</td>
            <td class="text-end">${m.avg_days}</td>
            <td class="text-end">${m.min_days}</td>
            <td class="text-end">${m.max_days}</td>
        </tr>
    `).join('');
}

export async function loadConversionTable() {
    const response = await fetch('/api/reports/service-metrics/referral-conversion' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#conversionTable tbody');
    if (data.metrics.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.metrics.map(m => `
        <tr>
            <td>${escapeHtml(m.service_type)}</td>
            <td class="text-end">${m.total_referrals}</td>
            <td class="text-end">${m.accepted}</td>
            <td class="text-end">${m.declined}</td>
            <td class="text-end">${m.pending}</td>
            <td class="text-end"><strong>${m.acceptance_rate}%</strong></td>
        </tr>
    `).join('');
}

export async function loadSubtypesTable() {
    const response = await fetch('/api/reports/service-subtypes' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#subtypesTable tbody');
    if (data.subtypes.length === 0) {
        tbody.innerHTML = '<tr><td colspan="3" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.subtypes.map(s => `
        <tr>
            <td>${escapeHtml(s.service_type)}</td>
            <td>${escapeHtml(s.service_subtype)}</td>
            <td class="text-end">${s.count}</td>
        </tr>
    `).join('');
}

export async function loadOutcomesTable() {
    const response = await fetch('/api/reports/case-outcomes' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#outcomesTable tbody');
    if (data.outcomes.length === 0) {
        tbody.innerHTML = '<tr><td colspan="3" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    const total = data.outcomes.reduce((sum, o) => sum + o.count, 0);
    tbody.innerHTML = data.outcomes.map(o => `
        <tr>
            <td>${escapeHtml(o.resolution_type || 'Unspecified')}</td>
            <td class="text-end">${o.count}</td>
            <td class="text-end">${((o.count / total) * 100).toFixed(1)}%</td>
        </tr>
    `).join('');
}
