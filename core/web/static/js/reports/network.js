/**
 * Network Tab Reports
 * 
 * Contains network analysis: provider charts, collaboration tables,
 * referral flows, top programs, provider leaderboard, service pathways.
 */

import { charts, colorSchemes, createChartSafely, showTableLoading, showTableEmpty, showTableError, escapeHtml } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

export async function loadNetworkReports() {
    if (charts.sendingProviders) return;
    
    await Promise.all([
        loadProviderCharts(),
        loadCollaborationTable(),
        loadReferralFlowSankey(),
        loadTopProgramsTable(),
        loadProviderLeaderboard(),
        loadServicePathways()
    ]);
}

export async function loadProviderCharts() {
    // Sending providers
    const sendingResponse = await fetch('/api/reports/sending-providers' + buildFilterQueryString());
    const sendingData = await sendingResponse.json();
    
    const sendingCtx = document.getElementById('sendingProvidersChart');
    if (charts.sendingProviders) charts.sendingProviders.destroy();
    
    charts.sendingProviders = new Chart(sendingCtx, {
        type: 'bar',
        data: {
            labels: sendingData.labels,
            datasets: [{
                label: 'Referrals Sent',
                data: sendingData.values,
                backgroundColor: colorSchemes.primary[1]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
    
    // Receiving providers
    const receivingResponse = await fetch('/api/reports/receiving-providers' + buildFilterQueryString());
    const receivingData = await receivingResponse.json();
    
    const receivingCtx = document.getElementById('receivingProvidersChart');
    if (charts.receivingProviders) charts.receivingProviders.destroy();
    
    charts.receivingProviders = new Chart(receivingCtx, {
        type: 'bar',
        data: {
            labels: receivingData.labels,
            datasets: [{
                label: 'Referrals Received',
                data: receivingData.values,
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

export async function loadCollaborationTable() {
    const response = await fetch('/api/reports/network/provider-collaboration' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#collaborationTable tbody');
    if (data.collaborations.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.collaborations.map(c => `
        <tr>
            <td>${escapeHtml(c.from)}</td>
            <td class="text-center"><i class="fas fa-arrow-right text-muted"></i></td>
            <td>${escapeHtml(c.to)}</td>
            <td class="text-end"><strong>${c.count}</strong></td>
        </tr>
    `).join('');
}

export async function loadReferralFlowSankey() {
    try {
        const minReferrals = document.getElementById('sankeyMinReferrals')?.value || 5;
        const filterString = buildFilterQueryString();
        const separator = filterString ? '&' : '?';
        const response = await fetch(`/api/reports/referral-flow-sankey${filterString}${separator}min_referrals=${minReferrals}`);
        const data = await response.json();
        
        const container = document.getElementById('referralFlowSankey');
        
        if (!data.nodes || data.nodes.length === 0) {
            container.innerHTML = '<div class="text-center text-muted p-5">No referral flow data available for the selected filters</div>';
            return;
        }
        
        const trace = {
            type: "sankey",
            orientation: "h",
            node: {
                pad: 15,
                thickness: 20,
                line: { color: "black", width: 0.5 },
                label: data.nodes.map(n => n.name),
                color: data.nodes.map((n, i) => {
                    const colors = ['#2c5282', '#38a169', '#dd6b20', '#805ad5', '#d53f8c', '#319795'];
                    return colors[i % colors.length];
                })
            },
            link: {
                source: data.links.map(l => l.source),
                target: data.links.map(l => l.target),
                value: data.links.map(l => l.value),
                label: data.links.map(l => l.label),
                color: data.links.map(() => 'rgba(200, 200, 200, 0.4)')
            }
        };
        
        const layout = {
            title: { text: "Patient Movement Between Providers", font: { size: 14 } },
            font: { size: 11 },
            height: 600,
            margin: { l: 10, r: 10, t: 40, b: 10 },
            plot_bgcolor: 'rgba(0,0,0,0)',
            paper_bgcolor: 'rgba(0,0,0,0)'
        };
        
        const config = {
            responsive: true,
            displayModeBar: true,
            displaylogo: false,
            modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'],
            toImageButtonOptions: {
                format: 'png',
                filename: 'referral_flow_sankey',
                height: 800,
                width: 1200,
                scale: 2
            }
        };
        
        Plotly.newPlot(container, [trace], layout, config);
    } catch (error) {
        console.error('Error loading Sankey diagram:', error);
        document.getElementById('referralFlowSankey').innerHTML = 
            '<div class="text-center text-danger p-5">Error loading referral flow data</div>';
    }
}

export async function loadTopProgramsTable() {
    const response = await fetch('/api/reports/top-programs' + buildFilterQueryString());
    const data = await response.json();
    
    const tbody = document.querySelector('#topProgramsTable tbody');
    if (data.programs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted">No data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.programs.map(p => `
        <tr>
            <td>${escapeHtml(p.program_name)}</td>
            <td class="text-end">${p.total_referrals}</td>
            <td class="text-end">${p.accepted_referrals}</td>
            <td class="text-end"><strong>${p.acceptance_rate}%</strong></td>
        </tr>
    `).join('');
}

export async function loadProviderLeaderboard() {
    const tbody = document.getElementById('providerLeaderboardBody');
    
    try {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center text-muted py-4"><i class="fas fa-spinner fa-spin me-2"></i>Loading leaderboard...</td></tr>';
        
        const response = await fetch('/api/reports/provider-performance' + buildFilterQueryString());
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (data.error) {
            tbody.innerHTML = `<tr><td colspan="8" class="text-center text-danger py-4"><i class="fas fa-exclamation-triangle me-2"></i>Error: ${escapeHtml(data.error)}</td></tr>`;
            return;
        }
        
        if (!data.providers || !Array.isArray(data.providers) || data.providers.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="text-center text-muted py-4"><i class="fas fa-info-circle me-2"></i>No provider data available. Run an ETL job to populate data.</td></tr>';
            return;
        }
        
        const providers = data.providers
            .filter(p => p && typeof p.total_cases === 'number' && p.total_cases >= 5)
            .sort((a, b) => {
                if (b.total_cases !== a.total_cases) return b.total_cases - a.total_cases;
                const aAvg = typeof a.avg_days === 'number' ? a.avg_days : Infinity;
                const bAvg = typeof b.avg_days === 'number' ? b.avg_days : Infinity;
                return aAvg - bAvg;
            });
        
        if (providers.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="text-center text-muted py-4"><i class="fas fa-info-circle me-2"></i>No providers with 5+ cases found. Lower the threshold or add more data.</td></tr>';
            return;
        }
        
        tbody.innerHTML = providers.map((p, index) => {
            const rankClass = index === 0 ? 'text-warning fw-bold' : index === 1 ? 'text-secondary' : index === 2 ? 'text-bronze' : '';
            const rankIcon = index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : '';
            
            const providerName = p.provider_name || 'Unknown Provider';
            const totalCases = p.total_cases || 0;
            const pendingCases = p.pending_cases || 0;
            const avgDays = (typeof p.avg_days === 'number' && p.avg_days !== null) ? p.avg_days.toFixed(1) : 'N/A';
            const minDays = (typeof p.min_days === 'number' && p.min_days !== null) ? p.min_days.toFixed(1) : 'N/A';
            const maxDays = (typeof p.max_days === 'number' && p.max_days !== null) ? p.max_days.toFixed(1) : 'N/A';
            const completionRate = (typeof p.completion_rate === 'number') ? p.completion_rate.toFixed(1) : '0.0';
            
            return `
            <tr class="text-dark">
                <td class="text-center ${rankClass}">
                    <span style="font-size: 1.1em;">${rankIcon || (index + 1)}</span>
                </td>
                <td><strong>${escapeHtml(providerName)}</strong></td>
                <td class="text-end">${totalCases}</td>
                <td class="text-end">${pendingCases}</td>
                <td class="text-end">${avgDays}</td>
                <td class="text-end text-success">${minDays}</td>
                <td class="text-end text-danger">${maxDays}</td>
                <td class="text-end"><strong>${completionRate}%</strong></td>
            </tr>
            `;
        }).join('');
        
    } catch (error) {
        console.error('[loadProviderLeaderboard] Error:', error);
        tbody.innerHTML = `<tr><td colspan="8" class="text-center text-danger py-4"><i class="fas fa-exclamation-circle me-2"></i>Error loading leaderboard: ${escapeHtml(error.message)}</td></tr>`;
    }
}

export async function loadServicePathways() {
    const tbody = document.querySelector('#pathwaysTable tbody');
    
    try {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted"><i class="fas fa-spinner fa-spin me-2"></i>Loading pathways...</td></tr>';
        
        const response = await fetch('/api/reports/service-pathways' + buildFilterQueryString());
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (data.error) {
            tbody.innerHTML = `<tr><td colspan="5" class="text-center text-danger"><i class="fas fa-exclamation-triangle me-2"></i>Error: ${escapeHtml(data.error)}</td></tr>`;
            return;
        }
        
        if (!data.pathways || !Array.isArray(data.pathways) || data.pathways.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted"><i class="fas fa-info-circle me-2"></i>No service pathways found. Data requires multiple services per case.</td></tr>';
            return;
        }
        
        tbody.innerHTML = data.pathways.map(p => {
            const initialService = p.initial_service || 'Unknown';
            const referralService = p.referral_service || 'Unknown';
            const count = p.count || 0;
            const avgDays = (typeof p.avg_days_between === 'number' && p.avg_days_between !== null) 
                ? p.avg_days_between.toFixed(1) 
                : 'N/A';
            
            return `
            <tr>
                <td>${escapeHtml(initialService)}</td>
                <td class="text-center"><i class="fas fa-arrow-right text-primary"></i></td>
                <td>${escapeHtml(referralService)}</td>
                <td class="text-end"><strong>${count}</strong></td>
                <td class="text-end">${avgDays}</td>
            </tr>
            `;
        }).join('');
        
    } catch (error) {
        console.error('[loadServicePathways] Error:', error);
        tbody.innerHTML = `<tr><td colspan="5" class="text-center text-danger"><i class="fas fa-exclamation-circle me-2"></i>Error loading pathways: ${escapeHtml(error.message)}</td></tr>`;
    }
}
