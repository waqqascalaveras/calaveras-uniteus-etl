/**
 * Outcomes Tab Reports
 * 
 * Contains outcome analysis: referral funnel, timing analysis, provider performance,
 * high-risk dropoff, client journey stages, service funnel, outcome metrics.
 */

import { charts, colorSchemes, createChartSafely, displayNoDataMessage, showTableLoading, showTableEmpty, showTableError, escapeHtml } from '../utils/utils.js';
import { buildFilterQueryString } from '../filters/filters.js';

export async function loadOutcomesReports() {
    if (charts.referralFunnel) return; // Already loaded
    
    try {
        await Promise.all([
            loadReferralFunnelAnalysis().catch(e => console.error('Funnel error:', e)),
            loadTimingAnalysis().catch(e => console.error('Timing error:', e)),
            loadProviderPerformanceMetrics().catch(e => console.error('Provider error:', e)),
            loadHighRiskDropOffAnalysis().catch(e => console.error('Risk error:', e)),
            loadClientJourneyStages().catch(e => console.error('Journey error:', e)),
            loadServiceFunnel().catch(e => console.error('Service funnel error:', e)),
            loadOutcomeMetrics().catch(e => console.error('Outcome metrics error:', e))
        ]);
    } catch (error) {
        console.error('Error loading outcomes reports:', error);
    }
}

export async function loadReferralFunnelAnalysis() {
    try {
        const filterQuery = buildFilterQueryString();
        const response = await fetch(`/api/reports/referral-funnel-analysis${filterQuery}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data.stages || data.stages.length === 0) {
            document.getElementById('completionRate').textContent = '0';
            document.getElementById('acceptanceRate').textContent = '0';
            return;
        }
    
        document.getElementById('completionRate').textContent = data.overall_completion_rate || 0;
        document.getElementById('acceptanceRate').textContent = data.acceptance_rate || 0;
        
        const ctx = document.getElementById('referralFunnelChart');
        if (!ctx) return;
        if (charts.referralFunnel) charts.referralFunnel.destroy();
        
        charts.referralFunnel = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.stages.map(s => s.stage),
                datasets: [{
                    label: 'Count',
                    data: data.stages.map(s => s.count),
                    backgroundColor: [
                        'rgba(54, 162, 235, 0.8)',
                        'rgba(75, 192, 192, 0.8)',
                        'rgba(153, 102, 255, 0.8)',
                        'rgba(255, 159, 64, 0.8)'
                    ],
                    borderColor: [
                        'rgba(54, 162, 235, 1)',
                        'rgba(75, 192, 192, 1)',
                        'rgba(153, 102, 255, 1)',
                        'rgba(255, 159, 64, 1)'
                    ],
                    borderWidth: 2
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
                                const stage = data.stages[context.dataIndex];
                                return [
                                    `Count: ${stage.count.toLocaleString()}`,
                                    `Percentage: ${stage.percentage}%`,
                                    `Dropped: ${stage.drop_from_previous.toLocaleString()}`
                                ];
                            }
                        }
                    }
                },
                scales: { 
                    y: { 
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value.toLocaleString();
                            }
                        }
                    } 
                }
            }
        });
        
        const dropCtx = document.getElementById('dropOffReasonsChart');
        if (charts.dropOffReasons) charts.dropOffReasons.destroy();
        
        if (data.drop_off_reasons && data.drop_off_reasons.length > 0) {
            charts.dropOffReasons = new Chart(dropCtx, {
                type: 'doughnut',
                data: {
                    labels: data.drop_off_reasons.map(r => r.reason),
                    datasets: [{
                        data: data.drop_off_reasons.map(r => r.count),
                        backgroundColor: [
                            'rgba(255, 99, 132, 0.8)',
                            'rgba(255, 206, 86, 0.8)',
                            'rgba(54, 162, 235, 0.8)',
                            'rgba(153, 102, 255, 0.8)'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { 
                            position: 'bottom',
                            labels: { font: { size: 11 }, padding: 8 }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const reason = data.drop_off_reasons[context.dataIndex];
                                    return [
                                        reason.reason,
                                        `Count: ${reason.count.toLocaleString()}`,
                                        `${reason.percentage}% of total`
                                    ];
                                }
                            }
                        }
                    }
                }
            });
        }
    } catch (error) {
        console.error('Error in referral funnel analysis:', error);
        document.getElementById('completionRate').textContent = '0';
        document.getElementById('acceptanceRate').textContent = '0';
    }
}

export async function loadTimingAnalysis() {
    try {
        const filterQuery = buildFilterQueryString();
        const response = await fetch(`/api/reports/referral-timing-analysis${filterQuery}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data.timing_stages || data.timing_stages.length === 0) {
            console.warn('No timing analysis data available');
            return;
        }
    
        const ctx = document.getElementById('timingAnalysisChart');
        if (!ctx) return;
        if (charts.timingAnalysis) charts.timingAnalysis.destroy();
        
        charts.timingAnalysis = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.timing_stages.map(s => s.stage),
                datasets: [
                    {
                        label: 'Min Days',
                        data: data.timing_stages.map(s => s.min_days),
                        backgroundColor: 'rgba(75, 192, 192, 0.6)',
                        borderColor: 'rgba(75, 192, 192, 1)',
                        borderWidth: 1
                    },
                    {
                        label: 'Avg Days',
                        data: data.timing_stages.map(s => s.avg_days),
                        backgroundColor: 'rgba(54, 162, 235, 0.6)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    },
                    {
                        label: 'Max Days',
                        data: data.timing_stages.map(s => s.max_days),
                        backgroundColor: 'rgba(255, 99, 132, 0.6)',
                        borderColor: 'rgba(255, 99, 132, 1)',
                        borderWidth: 1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top', labels: { font: { size: 11 } } },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return `${context.dataset.label}: ${context.parsed.y.toFixed(1)} days`;
                            }
                        }
                    }
                },
                scales: { 
                    y: { 
                        beginAtZero: true,
                        title: { display: true, text: 'Days' }
                    } 
                }
            }
        });
        
        const statsDiv = document.getElementById('timingStats');
        statsDiv.innerHTML = data.timing_stages.map(stage => `
            <div class="col-md-${12 / data.timing_stages.length}">
                <div class="border rounded p-2">
                    <small class="text-muted d-block">${stage.stage}</small>
                    <div class="d-flex justify-content-between mt-1">
                        <span class="badge bg-success">${stage.min_days} days</span>
                        <span class="badge bg-primary">${stage.avg_days} days</span>
                        <span class="badge bg-danger">${stage.max_days} days</span>
                    </div>
                    <small class="text-muted">${stage.count.toLocaleString()} referrals</small>
                </div>
            </div>
        `).join('');
    } catch (error) {
        console.error('Error in timing analysis:', error);
    }
}

export async function loadProviderPerformanceMetrics() {
    try {
        const providerType = document.querySelector('input[name="providerType"]:checked')?.value || 'receiving';
        const filterQuery = buildFilterQueryString();
        const separator = filterQuery ? '&' : '?';
        const response = await fetch(`/api/reports/provider-performance-metrics${filterQuery}${separator}provider_type=${providerType}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
    
        const tbody = document.getElementById('providerMetricsBody');
        if (!data.providers || data.providers.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-3">No data available</td></tr>';
            return;
        }
        
        tbody.innerHTML = data.providers.map(p => {
            const acceptClass = p.acceptance_rate >= 80 ? 'text-success' : p.acceptance_rate >= 60 ? 'text-warning' : 'text-danger';
            const completeClass = p.completion_rate >= 70 ? 'text-success' : p.completion_rate >= 50 ? 'text-warning' : 'text-danger';
            
            return `
                <tr>
                    <td class="small">${escapeHtml(p.provider_name)}</td>
                    <td class="text-end">
                        <span class="badge bg-secondary">${p.total_referrals}</span>
                    </td>
                    <td class="text-end">
                        <span class="badge ${acceptClass === 'text-success' ? 'bg-success' : acceptClass === 'text-warning' ? 'bg-warning' : 'bg-danger'}">
                            ${p.acceptance_rate}%
                        </span>
                    </td>
                    <td class="text-end">
                        <span class="badge ${completeClass === 'text-success' ? 'bg-success' : completeClass === 'text-warning' ? 'bg-warning' : 'bg-danger'}">
                            ${p.completion_rate}%
                        </span>
                    </td>
                    <td class="text-end small">${p.avg_response_days} days</td>
                </tr>
            `;
        }).join('');
    } catch (error) {
        console.error('Error in provider performance metrics:', error);
        const tbody = document.getElementById('providerMetricsBody');
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-3">Error loading data</td></tr>';
    }
}

export async function loadHighRiskDropOffAnalysis() {
    try {
        const filterQuery = buildFilterQueryString();
        const response = await fetch(`/api/reports/high-risk-drop-off-analysis${filterQuery}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
    
        const tbody = document.getElementById('highRiskBody');
        if (!data.service_types || data.service_types.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3" class="text-center text-muted py-3">No data available</td></tr>';
            return;
        }
        
        tbody.innerHTML = data.service_types.map(st => {
            const riskClass = st.drop_off_rate >= 40 ? 'danger' : st.drop_off_rate >= 25 ? 'warning' : 'success';
            
            return `
                <tr>
                    <td class="small">${escapeHtml(st.service_type)}</td>
                    <td class="text-end">
                        <small class="text-muted">${st.total_referrals}</small>
                    </td>
                    <td class="text-end">
                        <span class="badge bg-${riskClass}">${st.drop_off_rate}%</span>
                    </td>
                </tr>
            `;
        }).join('');
    } catch (error) {
        console.error('Error in high-risk drop-off analysis:', error);
        const tbody = document.getElementById('highRiskBody');
        tbody.innerHTML = '<tr><td colspan="3" class="text-center text-muted py-3">Error loading data</td></tr>';
    }
}

export async function loadClientJourneyStages() {
    try {
        const filterQuery = buildFilterQueryString();
        const response = await fetch(`/api/reports/client-journey-stages${filterQuery}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data.stages || data.stages.length === 0) {
            console.warn('No journey stages data available');
            return;
        }
    
        const ctx = document.getElementById('journeyStagesChart');
        if (!ctx) return;
        if (charts.journeyStages) charts.journeyStages.destroy();
        
        charts.journeyStages = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.stages.map(s => s.status),
                datasets: [{
                    label: 'Referral Count',
                    data: data.stages.map(s => s.count),
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const stage = data.stages[context.dataIndex];
                                return [
                                    `Count: ${stage.count.toLocaleString()}`,
                                    `Unique Clients: ${stage.unique_clients.toLocaleString()}`,
                                    `Avg Days: ${stage.avg_days_in_stage.toFixed(1)}`
                                ];
                            }
                        }
                    }
                },
                scales: { 
                    x: { 
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value.toLocaleString();
                            }
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error in client journey stages:', error);
    }
}

export async function loadServiceFunnel() {
    const filterQuery = buildFilterQueryString();
    const response = await fetch(`/api/reports/service-funnel${filterQuery}`);
    const data = await response.json();
    
    if (data.error) {
        console.error('Error loading service funnel:', data.error);
        return;
    }
    
    const ctx = document.getElementById('funnelChart');
    if (!ctx) return;
    if (charts.funnel) charts.funnel.destroy();
    
    charts.funnel = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.stages.map(s => s.name),
            datasets: [{
                label: 'Count',
                data: data.stages.map(s => s.count),
                backgroundColor: colorSchemes.primary,
                borderColor: colorSchemes.primary[0],
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
                            const stage = data.stages[context.dataIndex];
                            return [
                                `Count: ${stage.count.toLocaleString()}`,
                                `Percentage: ${stage.percentage}%`
                            ];
                        }
                    }
                }
            },
            scales: { y: { beginAtZero: true } }
        }
    });
}

export async function loadOutcomeMetrics() {
    const filterQuery = buildFilterQueryString();
    
    try {
        const response = await fetch(`/api/reports/outcome-metrics${filterQuery}`);
        
        if (!response.ok) {
            displayNoDataMessage('resolutionTypesChart', 'Error loading data');
            displayNoDataMessage('resolutionByServiceChart', 'Error loading data');
            return;
        }
        
        const data = await response.json();
        
        if (data.error) {
            displayNoDataMessage('resolutionTypesChart', 'Error loading data');
            displayNoDataMessage('resolutionByServiceChart', 'Error loading data');
            return;
        }
        
        // Resolution Types Chart
        const resCtx = document.getElementById('resolutionTypesChart');
        if (!resCtx) return;
        
        if (charts.resolutionTypes) charts.resolutionTypes.destroy();
        
        if (data.outcome_distribution && data.outcome_distribution.types && data.outcome_distribution.types.length > 0) {
            charts.resolutionTypes = new Chart(resCtx, {
                type: 'doughnut',
                data: {
                    labels: data.outcome_distribution.types,
                    datasets: [{
                        data: data.outcome_distribution.counts,
                        backgroundColor: colorSchemes.diverse
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: 'right' } }
                }
            });
        } else {
            displayNoDataMessage('resolutionTypesChart', 'No resolution data available');
        }
        
        // Resolution by Service Chart
        const servCtx = document.getElementById('resolutionByServiceChart');
        if (!servCtx) return;
        
        if (charts.resolutionByService) charts.resolutionByService.destroy();
        
        if (data.resolution_times && data.resolution_times.length > 0) {
            charts.resolutionByService = new Chart(servCtx, {
                type: 'bar',
                data: {
                    labels: data.resolution_times.map(r => r.service_type),
                    datasets: [{
                        label: 'Avg Days',
                        data: data.resolution_times.map(r => r.avg_days),
                        backgroundColor: colorSchemes.warm[2]
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
        } else {
            displayNoDataMessage('resolutionByServiceChart', 'No resolution time data available');
        }
    } catch (error) {
        console.error('[Outcome Metrics] Error:', error);
        displayNoDataMessage('resolutionTypesChart', 'Error loading data');
        displayNoDataMessage('resolutionByServiceChart', 'Error loading data');
    }
}
