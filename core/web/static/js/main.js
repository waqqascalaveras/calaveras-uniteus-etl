/**
 * Dashboard Main Initialization
 * 
 * Central entry point for the dashboard application.
 * Initializes all modules, sets up event listeners, and manages tab switching.
 */

import { loadOverviewReports, loadTimelineChart } from './charts/overview.js';
import { loadDemographicsReports } from './charts/demographics.js';
import { loadServicesReports } from './reports/services.js';
import { loadNetworkReports } from './reports/network.js';
import { loadPerformanceReports, loadCasesOverTimeChart } from './reports/performance.js';
import { loadOutcomesReports, loadProviderPerformanceMetrics } from './reports/outcomes.js';
import { loadGeographicReports } from './reports/geographic.js';
import { loadAdvancedReports, exportAllReports } from './reports/advanced.js';
import { 
    loadFilterOptions, 
    updateQuickFilterLabels, 
    applyGlobalFilters, 
    clearAllFilters,
    applyQuickFilter,
    removeFilter 
} from './filters/filters.js';

/**
 * Initialize dashboard on page load
 */
document.addEventListener('DOMContentLoaded', function() {
    console.log('Dashboard initializing...');
    
    // Check if ChartDataLabels is available
    if (typeof ChartDataLabels === 'undefined') {
        console.error('ChartDataLabels plugin not loaded! Check CDN connection.');
    } else {
        console.log('ChartDataLabels plugin loaded successfully');
    }
    
    // Initialize filter labels and options
    updateQuickFilterLabels();
    loadFilterOptions();
    
    // Load initial tab (Overview)
    loadOverviewReports();
    
    // Setup tab change listeners
    setupTabListeners();
    
    // Setup event listeners for controls
    setupControlListeners();
    
    // Expose functions to global scope for inline onclick handlers
    exposeGlobalFunctions();
    
    console.log('Dashboard initialized successfully');
});

/**
 * Setup tab change event listeners
 */
function setupTabListeners() {
    document.querySelectorAll('button[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function (event) {
            const targetId = event.target.getAttribute('data-bs-target');
            console.log(`Tab switched to: ${targetId}`);
            
            switch (targetId) {
                case '#demographics':
                    loadDemographicsReports();
                    break;
                case '#services':
                    loadServicesReports();
                    break;
                case '#network':
                    loadNetworkReports();
                    break;
                case '#performance':
                    loadPerformanceReports();
                    break;
                case '#outcomes':
                    loadOutcomesReports();
                    break;
                case '#geographic':
                    loadGeographicReports();
                    break;
                case '#advanced':
                    loadAdvancedReports();
                    break;
            }
        });
    });
}

/**
 * Setup control event listeners
 */
function setupControlListeners() {
    // Timeline grouping selector
    const timelineGrouping = document.getElementById('timelineGrouping');
    if (timelineGrouping) {
        timelineGrouping.addEventListener('change', loadTimelineChart);
    }
    
    // Cases over time grouping selector
    const casesTimeGrouping = document.getElementById('casesTimeGrouping');
    if (casesTimeGrouping) {
        casesTimeGrouping.addEventListener('change', loadCasesOverTimeChart);
    }
    
    // Provider type toggle for outcomes tab
    document.querySelectorAll('input[name="providerType"]').forEach(radio => {
        radio.addEventListener('change', function() {
            loadProviderPerformanceMetrics();
        });
    });
}

/**
 * Expose functions to global scope for inline onclick handlers
 * This maintains backward compatibility with existing HTML
 */
function exposeGlobalFunctions() {
    window.applyGlobalFilters = applyGlobalFilters;
    window.clearAllFilters = clearAllFilters;
    window.applyQuickFilter = applyQuickFilter;
    window.removeFilter = removeFilter;
    window.exportAllReports = exportAllReports;
}
