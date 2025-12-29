/**
 * Filter Management for Dashboard
 * 
 * This module handles all filtering logic including global filters,
 * quick date filters, and filter application across dashboard tabs.
 */

import { showError } from '../utils/utils.js';

// Global filters state
export let globalFilters = {
    startDate: null,
    endDate: null,
    caseStatus: null,
    serviceType: null,
    provider: null,
    program: null,
    gender: null,
    race: null
};

/**
 * Update quick filter button labels based on current date
 */
export function updateQuickFilterLabels() {
    const now = new Date();
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                        'July', 'August', 'September', 'October', 'November', 'December'];
    const quarterNames = ['Q1', 'Q2', 'Q3', 'Q4'];
    
    // This Month
    const thisMonthElem = document.getElementById('quickFilterThisMonth');
    if (thisMonthElem) {
        thisMonthElem.textContent = `This Month (${monthNames[now.getMonth()]})`;
    }
    
    // Last Month
    const lastMonthElem = document.getElementById('quickFilterLastMonth');
    if (lastMonthElem) {
        const lastMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
        lastMonthElem.textContent = `Last Month (${monthNames[lastMonthDate.getMonth()]})`;
    }
    
    // This Quarter
    const thisQuarterElem = document.getElementById('quickFilterThisQuarter');
    if (thisQuarterElem) {
        const currentQuarter = Math.floor(now.getMonth() / 3);
        thisQuarterElem.textContent = `This Quarter (${quarterNames[currentQuarter]})`;
    }
    
    // Last Quarter
    const lastQuarterElem = document.getElementById('quickFilterLastQuarter');
    if (lastQuarterElem) {
        const currentQuarter = Math.floor(now.getMonth() / 3);
        const lastQuarter = currentQuarter === 0 ? 3 : currentQuarter - 1;
        lastQuarterElem.textContent = `Last Quarter (${quarterNames[lastQuarter]})`;
    }
    
    // This Year
    const thisYearElem = document.getElementById('quickFilterThisYear');
    if (thisYearElem) {
        thisYearElem.textContent = `This Year (${now.getFullYear()})`;
    }
    
    // Last Year
    const lastYearElem = document.getElementById('quickFilterLastYear');
    if (lastYearElem) {
        lastYearElem.textContent = `Last Year (${now.getFullYear() - 1})`;
    }
}

/**
 * Load filter options from API
 */
export async function loadFilterOptions() {
    try {
        const response = await fetch('/api/reports/filter-options');
        const options = await response.json();
        
        // Populate filter dropdowns
        if (options.case_statuses) {
            const select = document.getElementById('filterCaseStatus');
            options.case_statuses.forEach(status => {
                const option = document.createElement('option');
                option.value = status;
                option.textContent = status;
                select.appendChild(option);
            });
        }
        
        if (options.service_types) {
            const select = document.getElementById('filterServiceType');
            options.service_types.forEach(type => {
                const option = document.createElement('option');
                option.value = type;
                option.textContent = type;
                select.appendChild(option);
            });
        }
        
        if (options.providers) {
            const select = document.getElementById('filterProvider');
            options.providers.forEach(provider => {
                const option = document.createElement('option');
                option.value = provider;
                option.textContent = provider;
                select.appendChild(option);
            });
        }
        
        if (options.programs) {
            const select = document.getElementById('filterProgram');
            options.programs.forEach(program => {
                const option = document.createElement('option');
                option.value = program;
                option.textContent = program;
                select.appendChild(option);
            });
        }
        
        if (options.genders) {
            const select = document.getElementById('filterGender');
            options.genders.forEach(gender => {
                const option = document.createElement('option');
                option.value = gender;
                option.textContent = gender;
                select.appendChild(option);
            });
        }
        
        if (options.races) {
            const select = document.getElementById('filterRace');
            options.races.forEach(race => {
                const option = document.createElement('option');
                option.value = race;
                option.textContent = race;
                select.appendChild(option);
            });
        }
        
        // Set default date range if available
        if (options.date_range && options.date_range.min && options.date_range.max) {
            document.getElementById('filterStartDate').value = options.date_range.min.split('T')[0];
            document.getElementById('filterEndDate').value = options.date_range.max.split('T')[0];
        }
    } catch (error) {
        console.error('Error loading filter options:', error);
        showError('Failed to load filter options');
    }
}

/**
 * Apply global filters from modal
 */
export function applyGlobalFilters() {
    globalFilters = {
        startDate: document.getElementById('filterStartDate').value || null,
        endDate: document.getElementById('filterEndDate').value || null,
        caseStatus: document.getElementById('filterCaseStatus').value || null,
        serviceType: document.getElementById('filterServiceType').value || null,
        provider: document.getElementById('filterProvider').value || null,
        program: document.getElementById('filterProgram').value || null,
        gender: document.getElementById('filterGender').value || null,
        race: document.getElementById('filterRace').value || null
    };
    
    // Clear quick filter highlight when user applies custom filters
    clearActiveQuickFilter();
    
    // Update active filters display
    updateActiveFiltersBadges();
    
    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('globalFiltersModal'));
    if (modal) modal.hide();
    
    // Reload current tab data
    reloadCurrentTabData();
}

/**
 * Clear all filters
 */
export function clearAllFilters() {
    globalFilters = {
        startDate: null,
        endDate: null,
        caseStatus: null,
        serviceType: null,
        provider: null,
        program: null,
        gender: null,
        race: null
    };
    
    // Clear form inputs
    document.getElementById('filterStartDate').value = '';
    document.getElementById('filterEndDate').value = '';
    document.getElementById('filterCaseStatus').value = '';
    document.getElementById('filterServiceType').value = '';
    document.getElementById('filterProvider').value = '';
    document.getElementById('filterProgram').value = '';
    document.getElementById('filterGender').value = '';
    document.getElementById('filterRace').value = '';
    
    // Clear active quick filter highlight
    clearActiveQuickFilter();
    
    updateActiveFiltersBadges();
    reloadCurrentTabData();
}

/**
 * Update active filters badge display
 */
export function updateActiveFiltersBadges() {
    const container = document.getElementById('activeFiltersBadges');
    container.innerHTML = '';
    
    const activeFilters = [];
    if (globalFilters.startDate) activeFilters.push({key: 'startDate', label: `Start: ${globalFilters.startDate}`});
    if (globalFilters.endDate) activeFilters.push({key: 'endDate', label: `End: ${globalFilters.endDate}`});
    if (globalFilters.caseStatus) activeFilters.push({key: 'caseStatus', label: `Status: ${globalFilters.caseStatus}`});
    if (globalFilters.serviceType) activeFilters.push({key: 'serviceType', label: `Service: ${globalFilters.serviceType}`});
    if (globalFilters.provider) activeFilters.push({key: 'provider', label: `Provider: ${globalFilters.provider}`});
    if (globalFilters.program) activeFilters.push({key: 'program', label: `Program: ${globalFilters.program}`});
    if (globalFilters.gender) activeFilters.push({key: 'gender', label: `Gender: ${globalFilters.gender}`});
    if (globalFilters.race) activeFilters.push({key: 'race', label: `Race: ${globalFilters.race}`});
    
    if (activeFilters.length > 0) {
        const wrapper = document.createElement('div');
        wrapper.className = 'd-flex flex-wrap gap-2 align-items-center';
        
        const label = document.createElement('small');
        label.className = 'text-muted fw-bold';
        label.textContent = 'Active Filters:';
        wrapper.appendChild(label);
        
        activeFilters.forEach(filter => {
            const badge = document.createElement('span');
            badge.className = 'badge bg-primary d-inline-flex align-items-center gap-1';
            badge.style.cursor = 'pointer';
            badge.innerHTML = `
                ${filter.label}
                <i class="fas fa-times" data-filter-key="${filter.key}" style="cursor: pointer;" title="Remove filter"></i>
            `;
            // Use event delegation for dynamically created badges
            badge.querySelector('i').addEventListener('click', () => removeFilter(filter.key));
            wrapper.appendChild(badge);
        });
        
        container.appendChild(wrapper);
    }
}

/**
 * Remove a specific filter
 * @param {string} filterKey - Key of filter to remove
 */
export function removeFilter(filterKey) {
    globalFilters[filterKey] = null;
    
    // Clear the corresponding form input
    const inputMap = {
        'startDate': 'filterStartDate',
        'endDate': 'filterEndDate',
        'caseStatus': 'filterCaseStatus',
        'serviceType': 'filterServiceType',
        'provider': 'filterProvider',
        'program': 'filterProgram',
        'gender': 'filterGender',
        'race': 'filterRace'
    };
    
    const inputId = inputMap[filterKey];
    if (inputId) {
        const input = document.getElementById(inputId);
        if (input) input.value = '';
    }
    
    updateActiveFiltersBadges();
    reloadCurrentTabData();
}

/**
 * Clear active quick filter highlight
 */
export function clearActiveQuickFilter() {
    document.querySelectorAll('.quick-filter-btn').forEach(btn => {
        btn.classList.remove('active', 'btn-primary');
        btn.classList.add('btn-outline-secondary');
    });
}

/**
 * Apply a quick date filter
 * @param {string} filterType - Type of quick filter
 */
export function applyQuickFilter(filterType) {
    const now = new Date();
    let startDate, endDate;
    
    // Calculate date ranges based on filter type
    switch(filterType) {
        case 'thisMonth':
            startDate = new Date(now.getFullYear(), now.getMonth(), 1);
            endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);
            break;
        case 'lastMonth':
            startDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
            endDate = new Date(now.getFullYear(), now.getMonth(), 0);
            break;
        case 'last30Days':
            startDate = new Date(now);
            startDate.setDate(now.getDate() - 30);
            endDate = now;
            break;
        case 'thisQuarter':
            const currentQuarter = Math.floor(now.getMonth() / 3);
            startDate = new Date(now.getFullYear(), currentQuarter * 3, 1);
            endDate = new Date(now.getFullYear(), (currentQuarter + 1) * 3, 0);
            break;
        case 'lastQuarter':
            const lastQuarter = Math.floor(now.getMonth() / 3) - 1;
            const year = lastQuarter < 0 ? now.getFullYear() - 1 : now.getFullYear();
            const quarter = lastQuarter < 0 ? 3 : lastQuarter;
            startDate = new Date(year, quarter * 3, 1);
            endDate = new Date(year, (quarter + 1) * 3, 0);
            break;
        case 'thisYear':
            startDate = new Date(now.getFullYear(), 0, 1);
            endDate = new Date(now.getFullYear(), 11, 31);
            break;
        case 'lastYear':
            startDate = new Date(now.getFullYear() - 1, 0, 1);
            endDate = new Date(now.getFullYear() - 1, 11, 31);
            break;
    }
    
    // Format dates as YYYY-MM-DD
    const formatDate = (date) => {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    };
    
    // Update global filters
    globalFilters.startDate = formatDate(startDate);
    globalFilters.endDate = formatDate(endDate);
    
    // Update form inputs
    document.getElementById('filterStartDate').value = globalFilters.startDate;
    document.getElementById('filterEndDate').value = globalFilters.endDate;
    
    // Highlight active quick filter button
    clearActiveQuickFilter();
    const activeBtn = document.querySelector(`.quick-filter-btn[data-filter="${filterType}"]`);
    if (activeBtn) {
        activeBtn.classList.remove('btn-outline-secondary');
        activeBtn.classList.add('btn-primary', 'active');
    }
    
    updateActiveFiltersBadges();
    reloadCurrentTabData();
}

/**
 * Reload data for currently active tab
 */
export function reloadCurrentTabData() {
    // Determine which tab is currently active and reload its data
    const activeTab = document.querySelector('.nav-link.active');
    if (!activeTab) {
        // Import and call dynamically to avoid circular dependencies
        import('../charts/overview.js').then(m => m.loadOverviewReports());
        return;
    }
    
    const targetId = activeTab.getAttribute('data-bs-target');
    
    if (targetId === '#overview' || !targetId) {
        import('../charts/overview.js').then(m => m.loadOverviewReports());
    } else if (targetId === '#demographics') {
        import('../charts/demographics.js').then(m => m.loadDemographicsReports());
    } else if (targetId === '#services') {
        import('../reports/services.js').then(m => m.loadServicesReports());
    } else if (targetId === '#network') {
        import('../reports/network.js').then(m => m.loadNetworkReports());
    } else if (targetId === '#performance') {
        import('../reports/performance.js').then(m => m.loadPerformanceReports());
    } else if (targetId === '#outcomes') {
        import('../reports/outcomes.js').then(m => m.loadOutcomesReports());
    } else if (targetId === '#geographic') {
        import('../reports/geographic.js').then(m => m.loadGeographicReports());
    } else if (targetId === '#advanced') {
        import('../reports/advanced.js').then(m => m.loadAdvancedReports());
    }
}

/**
 * Build filter query string for API calls
 * @returns {string} Query string with filters
 */
export function buildFilterQueryString() {
    const params = new URLSearchParams();
    
    if (globalFilters.startDate) params.append('start_date', globalFilters.startDate);
    if (globalFilters.endDate) params.append('end_date', globalFilters.endDate);
    if (globalFilters.caseStatus) params.append('case_status', globalFilters.caseStatus);
    if (globalFilters.serviceType) params.append('service_type', globalFilters.serviceType);
    if (globalFilters.provider) params.append('provider', globalFilters.provider);
    if (globalFilters.program) params.append('program', globalFilters.program);
    if (globalFilters.gender) params.append('gender', globalFilters.gender);
    if (globalFilters.race) params.append('race', globalFilters.race);
    
    const queryString = params.toString();
    return queryString ? '?' + queryString : '';
}
