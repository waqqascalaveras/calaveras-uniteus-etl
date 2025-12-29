/**
 * Geographic Tab Reports
 * 
 * Contains geographic distribution analysis and maps.
 */

import { charts, colorSchemes, createChartSafely, showTableLoading, showTableEmpty, showTableError, escapeHtml } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

export async function loadGeographicReports() {
    if (charts.cities) return; // Already loaded
    
    await Promise.all([
        loadGeographicDistribution()
    ]);
}

export async function loadGeographicDistribution() {
    const filterQuery = buildFilterQueryString();
    const response = await fetch(`/api/reports/geographic-distribution${filterQuery}`);
    const data = await response.json();
    
    if (data.error) {
        console.error('Error loading geographic distribution:', data.error);
        return;
    }
    
    // Cities Chart
    if (data.by_city && data.by_city.length > 0) {
        const citiesCtx = document.getElementById('citiesChart');
        if (charts.cities) charts.cities.destroy();
        
        charts.cities = new Chart(citiesCtx, {
            type: 'bar',
            data: {
                labels: data.by_city.map(c => c.city),
                datasets: [{
                    label: 'Cases',
                    data: data.by_city.map(c => c.cases),
                    backgroundColor: colorSchemes.primary[1]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { legend: { display: false } },
                scales: { x: { beginAtZero: true } }
            }
        });
    }
    
    // Counties Chart
    if (data.by_county && data.by_county.length > 0) {
        const countiesCtx = document.getElementById('countiesChart');
        if (charts.counties) charts.counties.destroy();
        
        charts.counties = new Chart(countiesCtx, {
            type: 'pie',
            data: {
                labels: data.by_county.map(c => c.county),
                datasets: [{
                    data: data.by_county.map(c => c.cases),
                    backgroundColor: colorSchemes.diverse
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'right' } }
            }
        });
    }
    
    // ZIP Table
    const tbody = document.querySelector('#zipTable tbody');
    if (data.by_zip && data.by_zip.length > 0) {
        tbody.innerHTML = data.by_zip.map(z => `
            <tr>
                <td>${escapeHtml(z.zip)}</td>
                <td class="text-end">${z.cases}</td>
            </tr>
        `).join('');
    } else {
        tbody.innerHTML = '<tr><td colspan="2" class="text-center text-muted">No data available</td></tr>';
    }
}
