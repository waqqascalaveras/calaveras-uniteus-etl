/**
 * Demographics Tab Charts and Reports
 * 
 * Contains demographic analysis charts: age, gender, race, geography,
 * household, income, insurance, communication preferences, marital status,
 * language, and military service.
 */

import { charts, colorSchemes, createChartSafely, validateChartData, displayNoDataMessage, showTableLoading, showTableEmpty, showTableError, escapeHtml } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

/**
 * Load all demographics tab reports
 */
export async function loadDemographicsReports() {
    if (charts.ageDist) return; // Already loaded
    
    await Promise.all([
        loadAgeDistributionChart(),
        loadGenderChart(),
        loadRaceChart(),
        loadGeographicTable(),
        loadHouseholdChart(),
        loadHouseholdScatterChart(),
        loadIncomeChart(),
        loadInsuranceChart(),
        loadCommPrefChart(),
        loadMaritalChart(),
        loadLanguageChart(),
        loadMilitaryCharts()
    ]);
}

export async function loadAgeDistributionChart() {
    const response = await fetch('/api/reports/demographics/age-distribution' + buildFilterQueryString());
    const data = await response.json();

    const ctx = document.getElementById('ageDistChart');
    if (!ctx) return;
    if (charts.ageDist) charts.ageDist.destroy();
    
    charts.ageDist = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.labels,
            datasets: [{
                label: 'Clients',
                data: data.values,
                backgroundColor: colorSchemes.primary
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { 
                legend: { display: false },
                datalabels: { display: false }  // Temporarily disabled
            },
            scales: { y: { beginAtZero: true } }
        }
    });
}

export async function loadGenderChart() {
    const response = await fetch('/api/reports/demographics/gender' + buildFilterQueryString());
    const data = await response.json();

    const ctx = document.getElementById('genderChart');
    if (!ctx) return;
    if (charts.gender) charts.gender.destroy();
    
    charts.gender = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.labels,
            datasets: [{
                data: data.values,
                backgroundColor: colorSchemes.diverse
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { 
                legend: { position: 'right' },
                datalabels: { display: false }  // Temporarily disabled
            }
        }
    });
}

export async function loadRaceChart() {
    const response = await fetch('/api/reports/demographics/race-ethnicity' + buildFilterQueryString());
    const data = await response.json();

    const ctx = document.getElementById('raceChart');
    if (!ctx) return;
    if (charts.race) charts.race.destroy();
    
    charts.race = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.labels,
            datasets: [{
                label: 'Clients',
                data: data.values,
                backgroundColor: colorSchemes.warm
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: { 
                legend: { display: false },
                datalabels: { display: false }  // Temporarily disabled
            },
            scales: { x: { beginAtZero: true } }
        }
    });
}

export async function loadGeographicTable() {
    const response = await fetch('/api/reports/geographic/cases-by-location' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#geoTable tbody');
    if (data.locations.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.locations.map(loc => `
        <tr>
            <td>${escapeHtml(loc.city)}</td>
            <td>${escapeHtml(loc.county)}</td>
            <td>${loc.state}</td>
            <td class="text-end">${loc.case_count}</td>
        </tr>
    `).join('');
}

export async function loadHouseholdChart() {
    const chartName = 'householdChart';
    try {
        const response = await fetch('/api/reports/demographics/household-composition' + buildFilterQueryString());
        
        if (!response.ok) {
            displayNoDataMessage(chartName, 'No data available');
            return;
        }
        
        const data = await response.json();
        const ctx = document.getElementById(chartName);
        if (!ctx) return;
        
        if (charts.household) charts.household.destroy();
        
        if (!data.labels || data.labels.length === 0) {
            displayNoDataMessage(chartName, 'No household data available');
            return;
        }
        
        charts.household = new Chart(ctx, {
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
                plugins: { 
                    legend: { display: false },
                    datalabels: { display: false }  // Temporarily disabled
                },
                scales: { x: { beginAtZero: true } }
            }
        });
    } catch (error) {
        console.error(`[${chartName}] Error:`, error);
        displayNoDataMessage(chartName, 'Error loading data');
    }
}

export async function loadHouseholdScatterChart() {
    const chartName = 'householdScatterChart';
    try {
        const response = await fetch('/api/reports/demographics/household-adults-children' + buildFilterQueryString());
        
        if (!response.ok) {
            displayNoDataMessage(chartName, 'No data available');
            return;
        }
        
        const data = await response.json();
        const ctx = document.getElementById(chartName);
        if (!ctx) return;
        
        if (charts.householdScatter) charts.householdScatter.destroy();
        
        if (!data.data || data.data.length === 0) {
            displayNoDataMessage(chartName, 'No household composition data available');
            return;
        }
        
        charts.householdScatter = new Chart(ctx, {
            type: 'bubble',
            data: {
                datasets: [{
                    label: 'Households',
                    data: data.data.map(d => ({
                        x: d.x,
                        y: d.y,
                        r: Math.sqrt(d.count) * 3
                    })),
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const dataPoint = data.data[context.dataIndex];
                                return `Adults: ${dataPoint.x}, Children: ${dataPoint.y} (${dataPoint.count} households)`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Adults in Household' },
                        beginAtZero: true,
                        ticks: { stepSize: 1 }
                    },
                    y: {
                        title: { display: true, text: 'Children in Household' },
                        beginAtZero: true,
                        ticks: { stepSize: 1 }
                    }
                }
            }
        });
    } catch (error) {
        console.error(`[${chartName}] Error:`, error);
        displayNoDataMessage(chartName, 'Error loading data');
    }
}

export async function loadIncomeChart() {
    try {
        const response = await fetch('/api/reports/demographics/income-distribution' + buildFilterQueryString());
        if (!response.ok) return;
        
        const data = await response.json();
        const ctx = document.getElementById('incomeChart');
        if (!ctx) return;
        
        if (charts.income) charts.income.destroy();
        if (!data.labels || data.labels.length === 0) return;
        
        charts.income = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Clients',
                    data: data.values,
                    backgroundColor: colorSchemes.primary
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { display: false },
                    datalabels: { display: false }  // Temporarily disabled
                },
                scales: { y: { beginAtZero: true } }
            }
        });
    } catch (error) {
        console.error('Error loading income chart:', error);
    }
}

export async function loadInsuranceChart() {
    try {
        const response = await fetch('/api/reports/demographics/insurance-coverage' + buildFilterQueryString());
        if (!response.ok) return;
        
        const data = await response.json();
        const ctx = document.getElementById('insuranceChart');
        if (!ctx) return;
        
        if (charts.insurance) charts.insurance.destroy();
        if (!data.labels || data.labels.length === 0) return;
        
        charts.insurance = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.values,
                    backgroundColor: colorSchemes.warm
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { position: 'right' },
                    datalabels: { display: false }  // Temporarily disabled
                }
            }
        });
    } catch (error) {
        console.error('Error loading insurance chart:', error);
    }
}

export async function loadCommPrefChart() {
    try {
        const response = await fetch('/api/reports/demographics/communication-preferences' + buildFilterQueryString());
        if (!response.ok) return;
        
        const data = await response.json();
        const ctx = document.getElementById('commPrefChart');
        if (!ctx) return;
        
        if (charts.commPref) charts.commPref.destroy();
        if (!data.labels || data.labels.length === 0) return;
        
        charts.commPref = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Clients',
                    data: data.values,
                    backgroundColor: colorSchemes.accent
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { 
                    legend: { display: false },
                    datalabels: { display: false }  // Temporarily disabled
                },
                scales: { x: { beginAtZero: true } }
            }
        });
    } catch (error) {
        console.error('Error loading communication preferences chart:', error);
    }
}

export async function loadMaritalChart() {
    try {
        const response = await fetch('/api/reports/demographics/marital-status' + buildFilterQueryString());
        if (!response.ok) return;
        
        const data = await response.json();
        const ctx = document.getElementById('maritalChart');
        if (!ctx) return;
        
        if (charts.marital) charts.marital.destroy();
        if (!data.labels || data.labels.length === 0) return;
        
        charts.marital = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.values,
                    backgroundColor: colorSchemes.cool
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { position: 'right' },
                    datalabels: { display: false }  // Temporarily disabled
                }
            }
        });
    } catch (error) {
        console.error('Error loading marital status chart:', error);
    }
}

export async function loadLanguageChart() {
    try {
        const response = await fetch('/api/reports/demographics/language-preferences' + buildFilterQueryString());
        if (!response.ok) return;
        
        const data = await response.json();
        const ctx = document.getElementById('languageChart');
        if (!ctx) return;
        
        if (charts.language) charts.language.destroy();
        if (!data.labels || data.labels.length === 0) return;
        
        charts.language = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Clients',
                    data: data.values,
                    backgroundColor: colorSchemes.primary
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { 
                    legend: { display: false },
                    datalabels: { display: false }  // Temporarily disabled
                },
                scales: { x: { beginAtZero: true } }
            }
        });
    } catch (error) {
        console.error('Error loading language preferences chart:', error);
    }
}

export async function loadMilitaryCharts() {
    const response = await fetch('/api/reports/military/veteran-services' + buildFilterQueryString());
    const data = await response.json();
    
    // Military Affiliation Chart
    const affCtx = document.getElementById('milAffChart');
    if (charts.milAff) charts.milAff.destroy();
    
    if (data.by_affiliation.labels.length > 0) {
        charts.milAff = new Chart(affCtx, {
            type: 'bar',
            data: {
                labels: data.by_affiliation.labels,
                datasets: [{
                    label: 'Requests',
                    data: data.by_affiliation.values,
                    backgroundColor: colorSchemes.success[2]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { display: false },
                    datalabels: { display: false }  // Temporarily disabled
                },
                scales: { y: { beginAtZero: true } }
            }
        });
    }
    
    // Military Branch Chart
    const branchCtx = document.getElementById('milBranchChart');
    if (charts.milBranch) charts.milBranch.destroy();
    
    if (data.by_branch.labels.length > 0) {
        charts.milBranch = new Chart(branchCtx, {
            type: 'doughnut',
            data: {
                labels: data.by_branch.labels,
                datasets: [{
                    data: data.by_branch.values,
                    backgroundColor: colorSchemes.diverse
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { position: 'right' },
                    datalabels: { display: false }  // Temporarily disabled
                }
            }
        });
    }
}
