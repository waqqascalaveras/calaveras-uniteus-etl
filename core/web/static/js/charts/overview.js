/**
 * Overview Tab Charts and Reports
 * 
 * This module contains chart loading functions for the Overview tab,
 * including summary cards, referral status, case status, service types, and timeline.
 */

import { colorSchemes, createChartSafely, validateChartData, sanitizeChartDataObject, createDatalabelsFormatter } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

/**
 * Load all overview tab reports
 */
export async function loadOverviewReports() {
    await Promise.all([
        loadSummaryCards(),
        loadReferralStatusChart(),
        loadCaseStatusChart(),
        loadServiceTypeChart(),
        loadTimelineChart()
    ]);
}

/**
 * Load summary cards with key metrics
 */
export async function loadSummaryCards() {
    try {
        const response = await fetch('/api/reports/summary' + buildFilterQueryString());
        const data = await response.json();
        
        // Display actual values from the API
        document.getElementById('totalReferrals').textContent = (data.total_referrals !== undefined && data.total_referrals !== null) ? data.total_referrals.toLocaleString() : 'N/A';
        document.getElementById('totalCases').textContent = (data.total_cases !== undefined && data.total_cases !== null) ? data.total_cases.toLocaleString() : 'N/A';
        document.getElementById('totalPeople').textContent = (data.total_people !== undefined && data.total_people !== null) ? data.total_people.toLocaleString() : 'N/A';
        document.getElementById('totalAssistance').textContent = (data.total_assistance_requests !== undefined && data.total_assistance_requests !== null) ? data.total_assistance_requests.toLocaleString() : 'N/A';
    } catch (error) {
        console.error('Error loading summary cards:', error);
        // Display error state instead of fake zeros
        document.getElementById('totalReferrals').textContent = 'Error';
        document.getElementById('totalCases').textContent = 'Error';
        document.getElementById('totalPeople').textContent = 'Error';
        document.getElementById('totalAssistance').textContent = 'Error';
    }
}

/**
 * Load referral status doughnut chart
 */
export async function loadReferralStatusChart() {
    const chartName = 'referralStatus';
    const canvasId = 'referralStatusChart';
    
    try {
        console.log(`[${chartName}] Fetching data...`);
        const response = await fetch('/api/reports/referral-status' + buildFilterQueryString());
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        let data = await response.json();
        console.log(`[${chartName}] Data received:`, data);

        // Sanitize data to handle null, zero, and invalid values
        data = sanitizeChartDataObject(data);

        // Validate data structure
        if (!validateChartData(data, chartName)) {
            console.warn(`[${chartName}] Invalid data, showing "No Data" chart`);
            
            // Create "No Data" chart
            const noDataConfig = {
                type: 'doughnut',
                data: {
                    labels: ['No Data'],
                    datasets: [{ 
                        data: [1], 
                        backgroundColor: ['#e0e0e0'],
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { 
                        legend: { position: 'right' }, 
                        tooltip: { enabled: false }, 
                        datalabels: { display: false }
                    }
                }
            };
            
            await createChartSafely(canvasId, chartName, noDataConfig);
            return;
        }

        // Create chart with valid data
        const chartConfig = {
            type: 'doughnut',
            data: { 
                labels: data.labels, 
                datasets: [{ 
                    data: data.values, 
                    backgroundColor: colorSchemes.diverse,
                    borderWidth: 2,
                    borderColor: '#fff'
                }] 
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: {
                    duration: 0  // Disable animations to prevent race conditions
                },
                plugins: {
                    legend: { 
                        position: 'right',
                        labels: {
                            padding: 10,
                            font: { size: 12 }
                        }
                    },
                    datalabels: {
                        display: false  // Temporarily disabled - compatibility issue with Chart.js 4.4.0
                    }
                }
            }
            // ChartDataLabels plugin removed temporarily
        };
        
        await createChartSafely(canvasId, chartName, chartConfig);
        console.log(`[${chartName}] ✓ Chart loaded successfully`);
        
    } catch (error) { 
        console.error(`[${chartName}] ✗ Error loading chart:`, error);
        console.error(`[${chartName}] Stack trace:`, error.stack);
    }
}

/**
 * Load case status pie chart
 */
export async function loadCaseStatusChart() {
    const chartName = 'caseStatus';
    const canvasId = 'caseStatusChart';
    
    try {
        console.log(`[${chartName}] Fetching data...`);
        const response = await fetch('/api/reports/case-status' + buildFilterQueryString());
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        let data = await response.json();
        console.log(`[${chartName}] Data received:`, data);

        // Sanitize data to handle null, zero, and invalid values
        data = sanitizeChartDataObject(data);

        // Validate data structure
        if (!validateChartData(data, chartName)) {
            console.warn(`[${chartName}] Invalid data, showing "No Data" chart`);
            
            // Create "No Data" chart
            const noDataConfig = {
                type: 'pie',
                data: {
                    labels: ['No Data'],
                    datasets: [{ 
                        data: [1], 
                        backgroundColor: ['#e0e0e0'],
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { 
                        legend: { position: 'right' }, 
                        tooltip: { enabled: false }, 
                        datalabels: { display: false }
                    }
                }
            };
            
            await createChartSafely(canvasId, chartName, noDataConfig);
            return;
        }

        // Create chart with valid data
        const chartConfig = {
            type: 'pie',
            data: { 
                labels: data.labels, 
                datasets: [{ 
                    data: data.values, 
                    backgroundColor: colorSchemes.diverse,
                    borderWidth: 2,
                    borderColor: '#fff'
                }] 
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: {
                    duration: 0  // Disable animations to prevent race conditions
                },
                plugins: {
                    legend: { 
                        position: 'right',
                        labels: {
                            padding: 10,
                            font: { size: 12 }
                        }
                    },
                    datalabels: {
                        display: false  // Temporarily disabled - compatibility issue with Chart.js 4.4.0
                    }
                }
            }
            // ChartDataLabels plugin removed temporarily
        };
        
        await createChartSafely(canvasId, chartName, chartConfig);
        console.log(`[${chartName}] ✓ Chart loaded successfully`);
        
    } catch (error) { 
        console.error(`[${chartName}] ✗ Error loading chart:`, error);
        console.error(`[${chartName}] Stack trace:`, error.stack);
    }
}

/**
 * Load service type horizontal bar chart
 */
export async function loadServiceTypeChart() {
    const chartName = 'serviceType';
    const canvasId = 'serviceTypeChart';
    
    try {
        console.log(`[${chartName}] Fetching data...`);
        const response = await fetch('/api/reports/service-types' + buildFilterQueryString());
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        let data = await response.json();
        console.log(`[${chartName}] Data received:`, data);

        // Sanitize data to handle null, zero, and invalid values
        data = sanitizeChartDataObject(data);

        // Validate data structure
        if (!validateChartData(data, chartName)) {
            console.warn(`[${chartName}] Invalid data, showing "No Data" chart`);
            
            const noDataConfig = {
                type: 'bar',
                data: {
                    labels: ['No Data'],
                    datasets: [{
                        label: 'Cases',
                        data: [0],
                        backgroundColor: '#e0e0e0'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    indexAxis: 'y',
                    plugins: { legend: { display: false }, datalabels: { display: false } },
                    scales: { x: { beginAtZero: true } }
                }
            };
            
            await createChartSafely(canvasId, chartName, noDataConfig);
            return;
        }
        
        const chartConfig = {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Cases',
                    data: data.values,
                    backgroundColor: colorSchemes.primary[0]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { 
                    legend: { display: false },
                    datalabels: {
                        display: false  // Temporarily disabled
                    }
                },
                scales: { x: { beginAtZero: true } }
            }
        };
        
        await createChartSafely(canvasId, chartName, chartConfig);
        console.log(`[${chartName}] ✓ Chart loaded successfully`);
        
    } catch (error) {
        console.error(`[${chartName}] ✗ Error loading chart:`, error);
        console.error(`[${chartName}] Stack trace:`, error.stack);
    }
}

/**
 * Load referrals timeline chart
 */
export async function loadTimelineChart() {
    const chartName = 'timeline';
    const canvasId = 'timelineChart';
    
    try {
        const grouping = document.getElementById('timelineGrouping').value;
        const filterString = buildFilterQueryString();
        const separator = filterString ? '&' : '?';
        
        console.log(`[${chartName}] Fetching data...`);
        const response = await fetch(`/api/reports/referrals-timeline?grouping=${grouping}${filterString ? separator + filterString.substring(1) : ''}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        let data = await response.json();
        console.log(`[${chartName}] Data received:`, data);

        // Sanitize data to handle null, zero, and invalid values
        data = sanitizeChartDataObject(data);

        // Validate data structure
        if (!validateChartData(data, chartName)) {
            console.warn(`[${chartName}] Invalid data, showing "No Data" chart`);
            
            const noDataConfig = {
                type: 'line',
                data: {
                    labels: ['No Data'],
                    datasets: [{
                        label: 'Referrals',
                        data: [0],
                        borderColor: '#e0e0e0',
                        backgroundColor: 'rgba(224, 224, 224, 0.1)'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { datalabels: { display: false } },
                    scales: { y: { beginAtZero: true } }
                }
            };
            
            await createChartSafely(canvasId, chartName, noDataConfig);
            return;
        }
        
        const chartConfig = {
            type: 'line',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Referrals',
                    data: data.values,
                    borderColor: colorSchemes.primary[0],
                    backgroundColor: 'rgba(44, 82, 130, 0.1)',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { datalabels: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        };
        
        await createChartSafely(canvasId, chartName, chartConfig);
        console.log(`[${chartName}] ✓ Chart loaded successfully`);
        
    } catch (error) {
        console.error(`[${chartName}] ✗ Error loading chart:`, error);
        console.error(`[${chartName}] Stack trace:`, error.stack);
    }
}
