/**
 * Performance Tab Reports
 * 
 * Contains performance metrics: workload analysis, client journey metrics,
 * cases over time.
 */

import { charts, colorSchemes, createChartSafely, showTableLoading, showTableEmpty, showTableError, escapeHtml } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

export async function loadPerformanceReports() {
    if (document.querySelector('#workloadTable tbody tr td')?.textContent !== 'Loading...') return;
    
    await Promise.all([
        loadWorkloadTable(),
        loadClientJourneyMetrics(),
        loadCasesOverTimeChart()
    ]);
}

export async function loadWorkloadTable() {
    const response = await fetch('/api/reports/workforce/employee-workload' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#workloadTable tbody');
    if (data.employees.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.employees.map(e => `
        <tr>
            <td>${escapeHtml(e.employee_name)}</td>
            <td>${escapeHtml(e.provider)}</td>
            <td class="text-end">${e.active_cases}</td>
            <td class="text-end">${e.total_cases}</td>
            <td class="text-end">${e.resolved_cases}</td>
            <td class="text-end"><strong>${e.resolution_rate}%</strong></td>
        </tr>
    `).join('');
}

export async function loadClientJourneyMetrics() {
    const response = await fetch('/api/reports/client-journey' + buildFilterQueryString());
    const data = await response.json();
    
    document.getElementById('journeyTotalClients').textContent = data.summary.total_clients.toLocaleString();
    document.getElementById('journeyAvgCases').textContent = data.summary.avg_cases_per_client;
    document.getElementById('journeyAvgReferrals').textContent = data.summary.avg_referrals_per_client;
    document.getElementById('journeyAvgAR').textContent = data.summary.avg_assistance_requests_per_client;
    
    const ctx = document.getElementById('touchpointChart');
    if (!ctx) return;
    if (charts.touchpoint) charts.touchpoint.destroy();
    
    charts.touchpoint = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.touchpoint_distribution.labels,
            datasets: [{
                label: 'Clients',
                data: data.touchpoint_distribution.values,
                backgroundColor: colorSchemes.primary[2]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { 
                legend: { display: false },
                title: { display: true, text: 'Number of Service Touchpoints per Client' }
            },
            scales: { y: { beginAtZero: true } }
        }
    });
}

export async function loadCasesOverTimeChart() {
    const grouping = document.getElementById('casesTimeGrouping').value;
    const filterString = buildFilterQueryString();
    const separator = filterString ? '&' : '?';
    const response = await fetch(`/api/reports/trends/cases-over-time?grouping=${grouping}${filterString ? separator + filterString.substring(1) : ''}`);
    const data = await response.json();
    
    const ctx = document.getElementById('casesOverTimeChart');
    if (!ctx) return;
    if (charts.casesOverTime) charts.casesOverTime.destroy();
    
    const statusColors = {
        'managed': colorSchemes.success[3],
        'active': colorSchemes.primary[2],
        'processed': colorSchemes.warm[3],
        'closed': colorSchemes.diverse[5],
        'off_platform': colorSchemes.diverse[6]
    };
    
    const datasets = data.datasets.map(ds => ({
        label: ds.label,
        data: ds.data,
        backgroundColor: statusColors[ds.label] || colorSchemes.diverse[0],
        borderColor: statusColors[ds.label] || colorSchemes.diverse[0],
        borderWidth: 2,
        fill: false,
        tension: 0.3
    }));
    
    charts.casesOverTime = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: { y: { beginAtZero: true, stacked: false } }
        }
    });
}
