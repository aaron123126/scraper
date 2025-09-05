// State management
let currentRun = null;
let currentPages = [];
let currentPage = 1;
let currentSearch = '';
let currentDomain = '';
let currentPreviewData = null;
let scrapeStatusInterval = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadRuns();
    loadScrapeConfig();
});

// View switching
function showView(viewName) {
    // Update nav links
    document.querySelectorAll('.nav-link').forEach(link => {
        link.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Update views
    document.querySelectorAll('.view').forEach(view => {
        view.classList.remove('active');
    });
    document.getElementById(`${viewName}View`).classList.add('active');
    
    // Load view data
    switch(viewName) {
        case 'runs':
            loadRuns();
            break;
        case 'search':
            document.getElementById('globalSearchInput').focus();
            break;
        case 'scraper':
            checkScrapeStatus();
            break;
    }
}

// Global search
async function performGlobalSearch(event) {
    if (event && event.type === 'keyup' && event.key !== 'Enter') {
        return;
    }
    
    const query = document.getElementById('globalSearchInput').value;
    if (!query) return;
    
    try {
        const response = await fetch(`/api/search?q=${encodeURIComponent(query)}&limit=100`);
        const data = await response.json();
        
        displayGlobalSearchResults(data.results);
    } catch (error) {
        console.error('Error performing search:', error);
    }
}

function displayGlobalSearchResults(results) {
    const container = document.getElementById('globalSearchResults');
    
    if (results.length === 0) {
        container.innerHTML = '<div class="no-results">No results found</div>';
        return;
    }
    
    container.innerHTML = `
        <div class="results-header">Found ${results.length} results</div>
        <div class="pages-list">
            ${results.map(result => `
                <div class="page-item" onclick="previewPage('${result.run_id}', '${result.hash}', '${escapeHtml(result.url)}')">
                    <div class="page-url">${escapeHtml(result.url)}</div>
                    <div class="page-meta">
                        <span><i class="fas fa-folder"></i> Run: ${result.run_id}</span>
                        <span><i class="fas fa-globe"></i> ${result.domain}</span>
                        <span><i class="fas fa-weight"></i> ${formatBytes(result.size)}</span>
                        <span><i class="fas fa-clock"></i> ${formatDate(result.timestamp)}</span>
                    </div>
                </div>
            `).join('')}
        </div>
    `;
}

// Scraping functionality
async function loadScrapeConfig() {
    try {
        const response = await fetch('/api/scrape/config');
        const config = await response.json();
        
        // Populate form with current config
        document.getElementById('startUrl').value = config.START_URL;
        document.getElementById('maxPages').value = config.MAX_PAGES;
        document.getElementById('maxDepth').value = config.MAX_DEPTH;
        document.getElementById('pagesPerDomain').value = config.PAGES_PER_DOMAIN;
        document.getElementById('maxWorkers').value = config.MAX_WORKERS;
        document.getElementById('requestDelay').value = config.REQUEST_DELAY;
        document.getElementById('imageQuality').value = config.IMAGE_QUALITY;
        document.getElementById('maxImageWidth').value = config.MAX_IMAGE_WIDTH;
        document.getElementById('skipAssets').checked = config.SKIP_ASSETS;
        document.getElementById('respectRobots').checked = config.RESPECT_ROBOTS_TXT;
    } catch (error) {
        console.error('Error loading config:', error);
    }
}

async function startScrape() {
    const config = {
        START_URL: document.getElementById('startUrl').value,
        MAX_PAGES: document.getElementById('maxPages').value,
        MAX_DEPTH: document.getElementById('maxDepth').value,
        PAGES_PER_DOMAIN: document.getElementById('pagesPerDomain').value,
        MAX_WORKERS: document.getElementById('maxWorkers').value,
        REQUEST_DELAY: document.getElementById('requestDelay').value,
        IMAGE_QUALITY: document.getElementById('imageQuality').value,
        MAX_IMAGE_WIDTH: document.getElementById('maxImageWidth').value,
        SKIP_ASSETS: document.getElementById('skipAssets').checked,
        RESPECT_ROBOTS_TXT: document.getElementById('respectRobots').checked
    };
    
    if (!config.START_URL) {
        alert('Please enter a Start URL');
        return;
    }
    
    try {
        const response = await fetch('/api/scrape/start', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        
        if (result.status === 'started') {
            document.getElementById('stopButton').style.display = 'inline-flex';
            document.getElementById('scrapeLog').style.display = 'block';
            
            // Start monitoring
            scrapeStatusInterval = setInterval(checkScrapeStatus, 2000);
        } else if (result.error) {
            alert(`Error: ${result.error}`);
        }
    } catch (error) {
        console.error('Error starting scrape:', error);
        alert('Failed to start scraping');
    }
}

async function stopScrape() {
    try {
        await fetch('/api/scrape/stop', {method: 'POST'});
        
        if (scrapeStatusInterval) {
            clearInterval(scrapeStatusInterval);
            scrapeStatusInterval = null;
        }
        
        document.getElementById('stopButton').style.display = 'none';
        document.getElementById('scrapeStatus').innerHTML = '<span class="status-idle">Idle</span>';
    } catch (error) {
        console.error('Error stopping scrape:', error);
    }
}

async function checkScrapeStatus() {
    try {
        const response = await fetch('/api/scrape/status');
        const status = await response.json();
        
        const statusEl = document.getElementById('scrapeStatus');
        
        if (status.status === 'running') {
            statusEl.innerHTML = '<span class="status-running"><i class="fas fa-spinner fa-spin"></i> Scraping in progress...</span>';
            document.getElementById('stopButton').style.display = 'inline-flex';
            
            if (status.log && status.log.length > 0) {
                document.getElementById('logContent').textContent = status.log.join('');
                document.getElementById('scrapeLog').style.display = 'block';
            }
        } else {
            statusEl.innerHTML = '<span class="status-idle">Ready</span>';
            document.getElementById('stopButton').style.display = 'none';
            
            if (scrapeStatusInterval) {
                clearInterval(scrapeStatusInterval);
                scrapeStatusInterval = null;
            }
        }
    } catch (error) {
        console.error('Error checking status:', error);
    }
}

// Page preview functionality
async function previewPage(runId, hash, url) {
    currentPreviewData = {runId, hash, url};
    
    // Show modal
    document.getElementById('previewModalTitle').textContent = url;
    document.getElementById('pagePreviewModal').classList.add('active');
    
    // Load page in iframe
    const iframe = document.getElementById('pagePreview');
    iframe.src = `/api/run/${runId}/preview/${hash}`;
    iframe.style.display = 'block';
    document.getElementById('pageCode').style.display = 'none';
}

async function toggleCode() {
    const iframe = document.getElementById('pagePreview');
    const codeView = document.getElementById('pageCode');
    
    if (iframe.style.display === 'none') {
        // Show preview
        iframe.style.display = 'block';
        codeView.style.display = 'none';
    } else {
        // Show code
        if (!codeView.textContent) {
            // Load code if not already loaded
            const response = await fetch(`/api/run/${currentPreviewData.runId}/page/${currentPreviewData.hash}`);
            const data = await response.json();
            codeView.textContent = data.content;
        }
        iframe.style.display = 'none';
        codeView.style.display = 'block';
    }
}

function openInNewTab() {
    if (currentPreviewData) {
        window.open(`/api/run/${currentPreviewData.runId}/preview/${currentPreviewData.hash}`, '_blank');
    }
}

function closePreviewModal() {
    document.getElementById('pagePreviewModal').classList.remove('active');
    currentPreviewData = null;
}

// Load all runs
async function loadRuns() {
    try {
        const response = await fetch('/api/runs');
        const runs = await response.json();
        
        displayRuns(runs);
        updateNavStats(runs);
    } catch (error) {
        console.error('Error loading runs:', error);
    }
}

// Display runs
function displayRuns(runs) {
    const container = document.getElementById('runsList');
    
    if (runs.length === 0) {
        container.innerHTML = '<div class="loading">No scraping runs found</div>';
        return;
    }
    
    container.innerHTML = runs.map(run => `
        <div class="run-card" onclick="loadRun('${run.id}')">
            <div class="run-card-header">
                <div>
                    <div class="run-date">${formatDate(run.timestamp)}</div>
                    <div class="run-url">${run.start_url}</div>
                </div>
            </div>
            <div class="run-stats">
                <div class="stat">
                    <span class="stat-label">Pages</span>
                    <span class="stat-value">${run.pages_scraped}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Duration</span>
                    <span class="stat-value">${formatDuration(run.stats.elapsed_seconds)}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Size</span>
                    <span class="stat-value">${formatBytes(run.stats.bytes_downloaded)}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Domains</span>
                    <span class="stat-value">${run.stats.total_domains || 0}</span>
                </div>
            </div>
        </div>
    `).join('');
}

// Update navigation stats
function updateNavStats(runs) {
    const totalPages = runs.reduce((sum, run) => sum + run.pages_scraped, 0);
    const totalSize = runs.reduce((sum, run) => sum + (run.stats.bytes_downloaded || 0), 0);
    
    document.getElementById('navStats').innerHTML = `
        <div><i class="fas fa-folder"></i> ${runs.length} runs</div>
        <div><i class="fas fa-file"></i> ${totalPages.toLocaleString()} pages</div>
        <div><i class="fas fa-database"></i> ${formatBytes(totalSize)}</div>
    `;
}

// Load specific run
async function loadRun(runId) {
    try {
        const response = await fetch(`/api/run/${runId}`);
        const data = await response.json();
        
        currentRun = data;
        document.getElementById('runTitle').textContent = data.metadata.start_url;
        
        // Switch view
        document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
        document.getElementById('runDetailsView').classList.add('active');
        
        // Load initial tab
        showTab('pages');
        
        // Populate domain filter
        const domains = Object.keys(data.metadata.domain_counts || {});
        const domainFilter = document.getElementById('domainFilter');
        domainFilter.innerHTML = '<option value="">All Domains</option>' + 
            domains.map(d => `<option value="${d}">${d}</option>`).join('');
            
    } catch (error) {
        console.error('Error loading run:', error);
    }
}

// Tab switching
function showTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(`${tabName}Tab`).classList.add('active');
    
    // Load tab data
    switch(tabName) {
        case 'pages':
            loadPages();
            break;
        case 'stats':
            loadStats();
            break;
        case 'domains':
            loadDomains();
            break;
    }
}

// Load pages
async function loadPages(page = 1) {
    if (!currentRun) return;
    
    currentPage = page;
    
    try {
        const params = new URLSearchParams({
            page: page,
            per_page: 50,
            search: currentSearch,
            domain: currentDomain
        });
        
        const response = await fetch(`/api/run/${currentRun.id}/pages?${params}`);
        const data = await response.json();
        
        displayPages(data.pages);
        displayPagination(data.pagination);
        
    } catch (error) {
        console.error('Error loading pages:', error);
    }
}

// Display pages
function displayPages(pages) {
    const container = document.getElementById('pagesList');
    
    if (pages.length === 0) {
        container.innerHTML = '<div class="loading">No pages found</div>';
        return;
    }
    
    container.innerHTML = pages.map(page => `
        <div class="page-item" onclick="previewPage('${currentRun.id}', '${page.hash}', '${escapeHtml(page.url)}')">
            <div class="page-url">${escapeHtml(page.url)}</div>
            <div class="page-meta">
                <span><i class="fas fa-globe"></i> ${page.domain}</span>
                <span><i class="fas fa-layer-group"></i> Depth: ${page.depth}</span>
                <span><i class="fas fa-file-code"></i> ${page.content_type}</span>
                <span><i class="fas fa-weight"></i> ${formatBytes(page.size)}</span>
                <span><i class="fas fa-clock"></i> ${formatDate(page.timestamp)}</span>
            </div>
        </div>
    `).join('');
}

// Display pagination
function displayPagination(pagination) {
    const container = document.getElementById('pagination');
    const pages = [];
    
    // Previous button
    pages.push(`
        <button onclick="loadPages(${pagination.page - 1})" 
                ${pagination.page === 1 ? 'disabled' : ''}>
            <i class="fas fa-chevron-left"></i>
        </button>
    `);
    
    // Page numbers
    const start = Math.max(1, pagination.page - 2);
    const end = Math.min(pagination.total_pages, pagination.page + 2);
    
    for (let i = start; i <= end; i++) {
        pages.push(`
            <button onclick="loadPages(${i})" 
                    class="${i === pagination.page ? 'active' : ''}">
                ${i}
            </button>
        `);
    }
    
    // Next button
    pages.push(`
        <button onclick="loadPages(${pagination.page + 1})" 
                ${pagination.page === pagination.total_pages ? 'disabled' : ''}>
            <i class="fas fa-chevron-right"></i>
        </button>
    `);
    
    container.innerHTML = pages.join('');
}

// Search pages
function searchPages() {
    currentSearch = document.getElementById('searchInput').value;
    loadPages(1);
}

// Filter by domain
function filterByDomain() {
    currentDomain = document.getElementById('domainFilter').value;
    loadPages(1);
}

// Copy content
async function copyContent() {
    if (!currentPreviewData) return;
    
    try {
        const response = await fetch(`/api/run/${currentPreviewData.runId}/page/${currentPreviewData.hash}`);
        const data = await response.json();
        
        navigator.clipboard.writeText(data.content);
        alert('Content copied to clipboard!');
    } catch (error) {
        console.error('Error copying content:', error);
    }
}

// Download content
async function downloadContent() {
    if (!currentPreviewData) return;
    
    try {
        const response = await fetch(`/api/run/${currentPreviewData.runId}/page/${currentPreviewData.hash}`);
        const data = await response.json();
        
        const blob = new Blob([data.content], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `page_${Date.now()}.${data.type}`;
        a.click();
        URL.revokeObjectURL(url);
    } catch (error) {
        console.error('Error downloading content:', error);
    }
}

// Load statistics
async function loadStats() {
    if (!currentRun) return;
    
    try {
        const response = await fetch(`/api/run/${currentRun.id}/stats`);
        const stats = await response.json();
        
        displayStats(stats);
        
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

// Display statistics
function displayStats(stats) {
    const container = document.getElementById('statsContent');
    
    container.innerHTML = `
        <div class="stat-card">
            <h3>Pages Scraped</h3>
            <div class="value">${stats.basic_stats.pages_scraped}</div>
        </div>
        <div class="stat-card">
            <h3>Failed Pages</h3>
            <div class="value">${stats.basic_stats.pages_failed}</div>
        </div>
        <div class="stat-card">
            <h3>Total Size</h3>
            <div class="value">${formatBytes(stats.basic_stats.bytes_downloaded)}</div>
        </div>
        <div class="stat-card">
            <h3>Scraping Speed</h3>
            <div class="value">${stats.basic_stats.pages_per_second.toFixed(2)}/s</div>
        </div>
        <div class="stat-card">
            <h3>Duration</h3>
            <div class="value">${formatDuration(stats.basic_stats.elapsed_seconds)}</div>
        </div>
        <div class="stat-card">
            <h3>Domains</h3>
            <div class="value">${stats.basic_stats.total_domains}</div>
        </div>
    `;
}

// Load domains
function loadDomains() {
    if (!currentRun) return;
    
    const domains = currentRun.metadata.domain_counts || {};
    const sortedDomains = Object.entries(domains)
        .sort((a, b) => b[1] - a[1]);
    
    const container = document.getElementById('domainsContent');
    container.innerHTML = `
        <div class="domain-list">
            ${sortedDomains.map(([domain, count]) => `
                <div class="domain-item">
                    <span class="domain-name">${domain}</span>
                    <span class="domain-count">${count}</span>
                </div>
            `).join('')}
        </div>
    `;
}

// Utility functions
function formatDate(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleString();
}

function formatDuration(seconds) {
    if (!seconds) return '0s';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
    return `${(seconds / 3600).toFixed(1)}h`;
}

function formatBytes(bytes) {
    if (!bytes) return '0 B';
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${sizes[i]}`;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Handle modal close on background click
document.getElementById('pagePreviewModal').addEventListener('click', (e) => {
    if (e.target.id === 'pagePreviewModal') {
        closePreviewModal();
    }
});

// Handle keyboard shortcuts
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closePreviewModal();
    }
});