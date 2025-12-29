/**
 * Advanced Tab Reports
 * 
 * Contains advanced analytics: household size, cohort analysis, referral network,
 * client risk factors, referrals time series, and export functionality.
 */

import { charts, colorSchemes, createChartSafely, displayNoDataMessage, showTableLoading, showTableEmpty, showTableError, showSuccess, showError, showInfo, escapeHtml, exportChart, exportTableToCSV } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

export async function loadAdvancedReports() {
    if (charts.cohort) return; // Already loaded
    
    await Promise.all([
        loadCohortAnalysis(),
        loadReferralNetwork(),
        loadClientRiskFactors(),
        loadReferralsTimeSeries(),
        loadHouseholdSizeChart()
    ]);
}

export async function loadHouseholdSizeChart() {
    const chartName = 'householdSizeChart';
    try {
        const response = await fetch('/api/reports/demographics/household-composition' + buildFilterQueryString());
        
        if (!response.ok) {
            displayNoDataMessage(chartName, 'Error loading data');
            return;
        }
        
        const data = await response.json();
        const ctx = document.getElementById(chartName);
        if (!ctx) return;
        
        if (charts.householdSize) charts.householdSize.destroy();
        
        if (!data.labels || data.labels.length === 0) {
            displayNoDataMessage(chartName, 'No household data available');
            return;
        }
        
        charts.householdSize = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Households',
                    data: data.values,
                    backgroundColor: colorSchemes.cool
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { legend: { display: false }, title: { display: false } },
                scales: { x: { beginAtZero: true } }
            }
        });
    } catch (error) {
        console.error(`[${chartName}] Error:`, error);
        displayNoDataMessage(chartName, 'Error loading data');
    }
}

export async function loadCohortAnalysis() {
    const filterQuery = buildFilterQueryString();
    const response = await fetch(`/api/reports/cohort-analysis${filterQuery}`);
    const data = await response.json();
    
    if (data.error) {
        console.error('Error loading cohort analysis:', data.error);
        return;
    }
    
    const ctx = document.getElementById('cohortChart');
    if (!ctx) return;
    if (charts.cohort) charts.cohort.destroy();
    
    if (data.cohorts && data.cohorts.length > 0) {
        charts.cohort = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.cohorts,
                datasets: [
                    {
                        label: 'Cohort Size',
                        data: data.cohort_sizes,
                        borderColor: colorSchemes.primary[0],
                        backgroundColor: 'rgba(44, 82, 130, 0.1)',
                        fill: false
                    },
                    {
                        label: 'Return Rate %',
                        data: data.return_rates,
                        borderColor: colorSchemes.success[3],
                        backgroundColor: 'rgba(56, 161, 105, 0.1)',
                        fill: false,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                scales: {
                    y: {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        beginAtZero: true,
                        title: { display: true, text: 'Cohort Size' }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        beginAtZero: true,
                        max: 100,
                        title: { display: true, text: 'Return Rate %' },
                        grid: { drawOnChartArea: false }
                    }
                }
            }
        });
    }
}

export async function loadReferralNetwork() {
    const filterQuery = buildFilterQueryString();
    const response = await fetch(`/api/reports/referral-network${filterQuery}`);
    const data = await response.json();
    
    if (data.error) {
        console.error('Error loading referral network:', data.error);
        return;
    }
    
    const tbody = document.querySelector('#networkTable tbody');
    if (!data.connections || data.connections.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.connections.map(c => `
        <tr>
            <td>${escapeHtml(c.source)}</td>
            <td class="text-center"><i class="fas fa-arrow-right text-muted"></i></td>
            <td>${escapeHtml(c.target)}</td>
            <td class="text-end"><strong>${c.referrals}</strong></td>
            <td class="text-end">${c.unique_clients}</td>
            <td class="text-end">${c.acceptance_rate}%</td>
        </tr>
    `).join('');
}

export async function loadClientRiskFactors() {
    const filterQuery = buildFilterQueryString();
    const response = await fetch(`/api/reports/client-risk-factors${filterQuery}`);
    const data = await response.json();
    
    if (data.error) {
        console.error('Error loading risk factors:', data.error);
        return;
    }
    
    // Housing Chart
    if (data.housing_impact && data.housing_impact.length > 0) {
        const housingCtx = document.getElementById('housingChart');
        if (charts.housing) charts.housing.destroy();
        
        charts.housing = new Chart(housingCtx, {
            type: 'bar',
            data: {
                labels: data.housing_impact.map(h => h.status),
                datasets: [{
                    label: 'Cases',
                    data: data.housing_impact.map(h => h.cases),
                    backgroundColor: colorSchemes.warm[2]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        });
    }
    
    // Household Chart (risk factors context)
    if (data.household_size && data.household_size.length > 0) {
        const householdCtx = document.getElementById('householdChart');
        if (charts.household) charts.household.destroy();
        
        charts.household = new Chart(householdCtx, {
            type: 'bar',
            data: {
                labels: data.household_size.map(h => h.category),
                datasets: [{
                    label: 'Cases',
                    data: data.household_size.map(h => h.cases),
                    backgroundColor: colorSchemes.success[3]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        });
    }
}

export async function loadReferralsTimeSeries() {
    const filterQuery = buildFilterQueryString();
    const response = await fetch(`/api/reports/time-series/referrals${filterQuery}`);
    const data = await response.json();
    
    if (data.error) {
        console.error('Error loading referrals time series:', data.error);
        return;
    }
    
    const ctx = document.getElementById('referralsTimeChart');
    if (!ctx) return;
    if (charts.referralsTime) charts.referralsTime.destroy();
    
    if (data.dates && data.dates.length > 0) {
        charts.referralsTime = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.dates,
                datasets: [{
                    label: 'Referrals',
                    data: data.counts,
                    borderColor: colorSchemes.primary[2],
                    backgroundColor: 'rgba(66, 153, 225, 0.1)',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: { y: { beginAtZero: true } }
            }
        });
    }
}

export async function exportAllReports() {
    showInfo('Preparing export... This may take a moment.');
    
    try {
        // Export all charts
        const chartIds = Object.keys(charts);
        for (const chartId of chartIds) {
            if (charts[chartId]) {
                const canvas = charts[chartId].canvas;
                exportChart(canvas.id, canvas.id.replace('Chart', ''));
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        
        // Export all visible tables
        const tables = document.querySelectorAll('table[id]');
        tables.forEach(table => {
            const tbody = table.querySelector('tbody');
            if (tbody && tbody.textContent.trim() !== 'Loading...') {
                exportTableToCSV(table.id, table.id.replace('Table', ''));
            }
        });
        
        showSuccess('All reports exported successfully!');
    } catch (error) {
        console.error('Error exporting reports:', error);
        showError('Failed to export some reports');
    }
}
