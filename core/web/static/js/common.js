/**
 * Common JavaScript functions for UniteUs ETL Web Interface
 */

// Global variables
let toastContainer = null;

// Initialize common functionality
document.addEventListener('DOMContentLoaded', function() {
    initializeToasts();
    checkSystemHealth();
    
    // Initialize tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Start health check polling with exponential backoff
    startHealthCheckPolling();
});

/**
 * Initialize toast notifications
 */
function initializeToasts() {
    // Create toast container if it doesn't exist
    if (!document.getElementById('toastContainer')) {
        const container = document.createElement('div');
        container.id = 'toastContainer';
        container.className = 'toast-container position-fixed top-0 end-0 p-3';
        container.style.zIndex = '9999';
        document.body.appendChild(container);
    }
    toastContainer = document.getElementById('toastContainer');
}

/**
 * Show success message
 */
function showSuccess(message, title = 'Success') {
    showToast(message, title, 'success');
}

/**
 * Show error message
 */
function showError(message, title = 'Error') {
    showToast(message, title, 'danger');
}

/**
 * Show warning message
 */
function showWarning(message, title = 'Warning') {
    showToast(message, title, 'warning');
}

/**
 * Show info message
 */
function showInfo(message, title = 'Information') {
    showToast(message, title, 'info');
}

/**
 * Generic toast function
 */
function showToast(message, title, type = 'info') {
    const toastId = 'toast-' + Date.now();
    const iconMap = {
        'success': 'fas fa-check-circle',
        'danger': 'fas fa-exclamation-triangle',
        'warning': 'fas fa-exclamation-circle',
        'info': 'fas fa-info-circle'
    };
    
    const toastHtml = `
        <div id="${toastId}" class="toast align-items-center text-bg-${type} border-0" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="d-flex">
                <div class="toast-body">
                    <div class="d-flex align-items-center">
                        <i class="${iconMap[type]} me-2"></i>
                        <div>
                            <strong class="me-auto">${title}</strong>
                            <div>${message}</div>
                        </div>
                    </div>
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
        </div>
    `;
    
    toastContainer.insertAdjacentHTML('beforeend', toastHtml);
    
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement, {
        autohide: true,
        delay: type === 'danger' ? 10000 : 5000 // Errors stay longer
    });
    
    toast.show();
    
    // Remove toast element after it's hidden
    toastElement.addEventListener('hidden.bs.toast', function() {
        toastElement.remove();
    });
}

/**
 * API Response Cache for LAN deployment (aggressive caching)
 * Memory-safe with size limits and automatic cleanup
 */
const apiCache = {
    cache: new Map(),
    defaultTTL: 120000, // 2 minutes default (session-based)
    maxSize: 500, // Maximum number of cached entries to prevent memory leaks
    cleanupInterval: null, // Store interval ID for cleanup
    
    get(key) {
        const entry = this.cache.get(key);
        if (!entry) return null;
        
        // Check if expired
        if (Date.now() > entry.expires) {
            this.cache.delete(key);
            return null;
        }
        
        return entry.data;
    },
    
    set(key, data, ttl = this.defaultTTL) {
        // Enforce size limit - remove oldest entries if at limit
        if (this.cache.size >= this.maxSize) {
            this._evictOldest();
        }
        
        this.cache.set(key, {
            data: data,
            expires: Date.now() + ttl,
            created: Date.now() // Track creation time for eviction
        });
    },
    
    clear() {
        this.cache.clear();
    },
    
    // Evict oldest entries when cache is full
    _evictOldest() {
        // Remove 20% of oldest entries
        const entriesToRemove = Math.floor(this.maxSize * 0.2);
        const sortedEntries = Array.from(this.cache.entries())
            .sort((a, b) => a[1].created - b[1].created);
        
        for (let i = 0; i < entriesToRemove && i < sortedEntries.length; i++) {
            this.cache.delete(sortedEntries[i][0]);
        }
    },
    
    // Clear expired entries
    cleanup() {
        const now = Date.now();
        let removed = 0;
        for (const [key, entry] of this.cache.entries()) {
            if (now > entry.expires) {
                this.cache.delete(key);
                removed++;
            }
        }
        
        // If still over limit after cleanup, evict oldest
        if (this.cache.size > this.maxSize) {
            this._evictOldest();
        }
        
        return removed;
    },
    
    // Get cache statistics
    getStats() {
        return {
            size: this.cache.size,
            maxSize: this.maxSize,
            entries: Array.from(this.cache.keys())
        };
    }
};

// Cleanup cache every 5 minutes - store interval ID for cleanup
apiCache.cleanupInterval = setInterval(() => {
    const removed = apiCache.cleanup();
    if (removed > 0) {
        console.debug(`API cache cleanup: removed ${removed} expired entries`);
    }
}, 300000);

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    if (apiCache.cleanupInterval) {
        clearInterval(apiCache.cleanupInterval);
    }
    apiCache.clear();
});

/**
 * Cached fetch - Wrapper for fetch API with caching for GET requests
 * Optimized for LAN deployment with aggressive caching
 */
async function cachedFetch(url, options = {}, cacheTTL = 300000) {
    // Only cache GET requests
    if (options.method && options.method !== 'GET') {
        return safeFetch(url, options);
    }
    
    // Build cache key from full URL (including query string if present)
    let cacheKey = url;
    if (url.includes('?')) {
        // URL already has query string, use as-is
        cacheKey = url;
    } else if (options.body) {
        // Add query string from body if provided
        const params = new URLSearchParams(options.body);
        if (params.toString()) {
            cacheKey = `${url}?${params.toString()}`;
        }
    }
    
    // Check cache first
    const cached = apiCache.get(cacheKey);
    if (cached) {
        // Return cached response as a Response-like object
        return {
            ok: true,
            status: 200,
            json: async () => cached,
            text: async () => JSON.stringify(cached),
            headers: new Headers({ 'X-Cache': 'HIT' }),
            clone: function() { return this; }
        };
    }
    
    // Fetch from server
    const response = await safeFetch(url, options);
    
    if (response.ok) {
        try {
            const data = await response.json();
            // Cache successful responses
            apiCache.set(cacheKey, data, cacheTTL);
            // Return response with cached data
            return {
                ok: true,
                status: response.status,
                json: async () => data,
                text: async () => JSON.stringify(data),
                headers: response.headers,
                clone: function() { return this; }
            };
        } catch (e) {
            // Not JSON, return original response
            return response;
        }
    }
    
    return response;
}

/**
 * Clear API cache (useful when data changes)
 */
function clearApiCache(pattern = null) {
    if (!pattern) {
        apiCache.clear();
        return;
    }
    
    // Clear entries matching pattern
    for (const key of apiCache.cache.keys()) {
        if (key.includes(pattern)) {
            apiCache.cache.delete(key);
        }
    }
}

/**
 * Wrapper for fetch API with error handling
 */
async function safeFetch(url, options = {}) {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
        
        const response = await fetch(url, {
            ...options,
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        return response;
        
    } catch (error) {
        if (error.name === 'AbortError') {
            throw new Error('Request timeout - server may be down');
        }
        throw error;
    }
}

/**
 * Check system health and update status indicator
 */
let serverDownNotified = false;
let consecutiveHealthCheckFailures = 0;
let healthCheckIntervalId = null;
let currentHealthCheckInterval = 30000; // Start with 30 seconds
const MIN_HEALTH_CHECK_INTERVAL = 30000; // 30 seconds minimum
const MAX_HEALTH_CHECK_INTERVAL = 300000; // 5 minutes maximum

function startHealthCheckPolling() {
    // Clear any existing interval
    if (healthCheckIntervalId) {
        clearInterval(healthCheckIntervalId);
        healthCheckIntervalId = null;
    }
    
    // Start polling at current interval
    healthCheckIntervalId = setInterval(checkSystemHealth, currentHealthCheckInterval);
}

// Cleanup health check on page unload
window.addEventListener('beforeunload', () => {
    if (healthCheckIntervalId) {
        clearInterval(healthCheckIntervalId);
        healthCheckIntervalId = null;
    }
});

async function checkSystemHealth() {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000); // 3 second timeout
        
        const response = await fetch('/api/health', {
            signal: controller.signal,
            cache: 'no-cache' // Prevent caching
        });
        clearTimeout(timeoutId);
        
        const health = await response.json();
        
        // Reset failure counter on success
        if (consecutiveHealthCheckFailures > 0) {
            consecutiveHealthCheckFailures = 0;
            serverDownNotified = false;
            
            // Reset interval to minimum on recovery
            if (currentHealthCheckInterval !== MIN_HEALTH_CHECK_INTERVAL) {
                currentHealthCheckInterval = MIN_HEALTH_CHECK_INTERVAL;
                startHealthCheckPolling();
                console.log('Server recovered - health check interval reset to 30s');
            }
        }
        
        const statusIndicator = document.getElementById('statusIndicator');
        if (statusIndicator) {
            const newContent = '<i class="fas fa-circle text-success me-1"></i>System Ready';
            // Only update if content has changed to avoid unnecessary DOM updates
            if (statusIndicator.innerHTML !== newContent) {
                statusIndicator.innerHTML = newContent;
            }
        }
        
    } catch (error) {
        consecutiveHealthCheckFailures++;
        
        console.error('Health check failed:', error);
        const statusIndicator = document.getElementById('statusIndicator');
        if (statusIndicator) {
            const newContent = '<i class="fas fa-circle text-danger me-1"></i>Server Offline';
            if (statusIndicator.innerHTML !== newContent) {
                statusIndicator.innerHTML = newContent;
            }
        }
        
        // Only show error toast once after 2 consecutive failures
        if (consecutiveHealthCheckFailures >= 2 && !serverDownNotified) {
            serverDownNotified = true;
            showError(
                'The server is not responding. Please check if the server is running.',
                'Server Connection Lost'
            );
        }
        
        // Implement exponential backoff for failed health checks
        if (consecutiveHealthCheckFailures >= 3 && currentHealthCheckInterval < MAX_HEALTH_CHECK_INTERVAL) {
            // Double the interval (exponential backoff), capped at maximum
            currentHealthCheckInterval = Math.min(currentHealthCheckInterval * 2, MAX_HEALTH_CHECK_INTERVAL);
            startHealthCheckPolling();
            console.log(`Health check interval increased to ${currentHealthCheckInterval/1000}s after ${consecutiveHealthCheckFailures} failures`);
        }
    }
}

/**
 * Format numbers with locale-specific formatting
 */
function formatNumber(num) {
    if (num == null || num === undefined) return 'N/A';
    return Number(num).toLocaleString();
}

/**
 * Format file sizes
 */
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * Format duration from milliseconds
 */
function formatDuration(milliseconds) {
    if (!milliseconds || milliseconds < 0) return 'N/A';
    
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) {
        return `${days}d ${hours % 24}h ${minutes % 60}m`;
    } else if (hours > 0) {
        return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
        return `${minutes}m ${seconds % 60}s`;
    } else {
        return `${seconds}s`;
    }
}

/**
 * Truncate text with ellipsis
 */
function truncateText(text, maxLength = 100) {
    if (!text || text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    if (!text) return '';
    
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    
    return text.replace(/[&<>"']/g, function(m) { 
        return map[m]; 
    });
}

/**
 * Get status color class for Bootstrap
 */
function getStatusColor(status) {
    if (!status) return 'secondary';
    
    switch (status.toLowerCase()) {
        case 'completed':
        case 'success':
        case 'successful':
            return 'success';
        case 'running':
        case 'in_progress':
        case 'processing':
            return 'primary';
        case 'failed':
        case 'error':
        case 'failed_validation':
            return 'danger';
        case 'cancelled':
        case 'canceled':
        case 'warning':
            return 'warning';
        case 'pending':
        case 'queued':
            return 'info';
        default:
            return 'secondary';
    }
}

/**
 * Get icon for file type
 */
function getFileTypeIcon(filename) {
    if (!filename) return 'fas fa-file';
    
    const extension = filename.split('.').pop().toLowerCase();
    
    switch (extension) {
        case 'csv':
            return 'fas fa-file-csv';
        case 'txt':
            return 'fas fa-file-alt';
        case 'tsv':
            return 'fas fa-file-alt';
        case 'json':
            return 'fas fa-file-code';
        case 'xml':
            return 'fas fa-file-code';
        case 'pdf':
            return 'fas fa-file-pdf';
        case 'doc':
        case 'docx':
            return 'fas fa-file-word';
        case 'xls':
        case 'xlsx':
            return 'fas fa-file-excel';
        default:
            return 'fas fa-file';
    }
}

/**
 * Show loading spinner
 */
function showLoading(elementId, message = 'Loading...') {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = `
            <div class="text-center py-4">
                <div class="spinner-border text-primary mb-3" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p class="text-muted">${message}</p>
            </div>
        `;
    }
}

/**
 * Hide loading spinner
 */
function hideLoading(elementId) {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = '';
    }
}

/**
 * Confirm dialog with custom message
 */
function confirmDialog(message, onConfirm, onCancel = null) {
    if (confirm(message)) {
        if (typeof onConfirm === 'function') {
            onConfirm();
        }
        return true;
    } else {
        if (typeof onCancel === 'function') {
            onCancel();
        }
        return false;
    }
}

/**
 * Copy text to clipboard
 */
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        showSuccess('Copied to clipboard');
    } catch (err) {
        console.error('Failed to copy text: ', err);
        showError('Failed to copy to clipboard');
    }
}

/**
 * Download content as file
 */
function downloadFile(content, filename, contentType = 'text/plain') {
    const blob = new Blob([content], { type: contentType });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
}

/**
 * Debounce function calls
 */
function debounce(func, wait, immediate) {
    var timeout;
    return function() {
        var context = this, args = arguments;
        var later = function() {
            timeout = null;
            if (!immediate) func.apply(context, args);
        };
        var callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func.apply(context, args);
    };
}

/**
 * Throttle function calls
 */
function throttle(func, limit) {
    var inThrottle;
    return function() {
        var args = arguments;
        var context = this;
        if (!inThrottle) {
            func.apply(context, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    }
}

/**
 * Format date for display
 */
function formatDate(dateString, includeTime = true) {
    if (!dateString) return 'N/A';
    
    const date = new Date(dateString);
    if (isNaN(date.getTime())) return 'Invalid Date';
    
    const options = {
        year: 'numeric',
        month: 'short',
        day: 'numeric'
    };
    
    if (includeTime) {
        options.hour = '2-digit';
        options.minute = '2-digit';
        options.second = '2-digit';
    }
    
    return date.toLocaleDateString('en-US', options);
}

/**
 * Get relative time (e.g., "2 hours ago")
 */
function getRelativeTime(dateString) {
    if (!dateString) return 'N/A';
    
    const date = new Date(dateString);
    const now = new Date();
    const diffInSeconds = Math.floor((now - date) / 1000);
    
    if (diffInSeconds < 60) {
        return 'Just now';
    } else if (diffInSeconds < 3600) {
        const minutes = Math.floor(diffInSeconds / 60);
        return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
    } else if (diffInSeconds < 86400) {
        const hours = Math.floor(diffInSeconds / 3600);
        return `${hours} hour${hours > 1 ? 's' : ''} ago`;
    } else {
        const days = Math.floor(diffInSeconds / 86400);
        return `${days} day${days > 1 ? 's' : ''} ago`;
    }
}

/**
 * Check if element is in viewport
 */
function isInViewport(element) {
    const rect = element.getBoundingClientRect();
    return (
        rect.top >= 0 &&
        rect.left >= 0 &&
        rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
        rect.right <= (window.innerWidth || document.documentElement.clientWidth)
    );
}

/**
 * Smooth scroll to element
 */
function scrollToElement(elementId, offset = 0) {
    const element = document.getElementById(elementId);
    if (element) {
        const elementPosition = element.getBoundingClientRect().top;
        const offsetPosition = elementPosition + window.pageYOffset - offset;
        
        window.scrollTo({
            top: offsetPosition,
            behavior: 'smooth'
        });
    }
}

// Global error handler for unhandled promise rejections
window.addEventListener('unhandledrejection', function(event) {
    console.error('Unhandled promise rejection:', event.reason);
    
    // Check if it's a network/fetch error (server offline)
    const error = event.reason;
    if (error instanceof TypeError && 
        (error.message.includes('fetch') || error.message.includes('Failed to fetch') || 
         error.message.includes('NetworkError') || error.message.includes('Load failed'))) {
        showError('Server is not running.', 'Server Offline');
    } else if (error && error.name === 'AbortError') {
        // Fetch was aborted, likely timeout - treat as server offline
        showError('Server is not running.', 'Server Offline');
    } else {
        showError('An unexpected error occurred. Please try again.');
    }
});

// Note: Health check polling is now managed by startHealthCheckPolling() 
// called in DOMContentLoaded, with exponential backoff (30s-5min)
// called in DOMContentLoaded, with exponential backoff (30s-5min)