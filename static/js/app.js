// NFOGuard Web Interface JavaScript

// Global state
let currentTab = 'dashboard';
let currentMoviesPage = 1;
let currentSeriesPage = 1;
let dashboardData = null;

// Initialize app
document.addEventListener('DOMContentLoaded', function() {
    initializeTabs();
    initializeEventListeners();
    loadDashboard();
    loadSeriesSources();
});

// Tab management
function initializeTabs() {
    const tabButtons = document.querySelectorAll('.nav-tab');
    const tabContents = document.querySelectorAll('.tab-content');
    
    tabButtons.forEach(button => {
        button.addEventListener('click', function() {
            const tabName = this.dataset.tab;
            switchTab(tabName);
        });
    });
}

function switchTab(tabName) {
    // Update button states
    document.querySelectorAll('.nav-tab').forEach(btn => btn.classList.remove('active'));
    document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
    
    // Update content
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    document.getElementById(tabName).classList.add('active');
    
    currentTab = tabName;
    
    // Load tab-specific data
    switch(tabName) {
        case 'dashboard':
            loadDashboard();
            break;
        case 'movies':
            loadMovies();
            break;
        case 'tv':
            loadSeries();
            break;
        case 'reports':
            loadReport();
            break;
        case 'tools':
            loadDetailedStats();
            break;
    }
}

// Event listeners
function initializeEventListeners() {
    // Search inputs
    document.getElementById('movies-search').addEventListener('input', debounce(loadMovies, 500));
    document.getElementById('movies-imdb-search').addEventListener('input', debounce(loadMovies, 500));
    document.getElementById('series-search').addEventListener('input', debounce(loadSeries, 500));
    document.getElementById('series-imdb-search').addEventListener('input', debounce(loadSeries, 500));
    
    // Filter dropdowns
    document.getElementById('movies-filter-date').addEventListener('change', loadMovies);
    document.getElementById('movies-filter-source').addEventListener('change', loadMovies);
    document.getElementById('series-filter-date').addEventListener('change', loadSeries);
    document.getElementById('series-filter-source').addEventListener('change', loadSeries);
    
    // Forms
    document.getElementById('edit-form').addEventListener('submit', handleEditSubmit);
    document.getElementById('bulk-update-form').addEventListener('submit', handleBulkUpdate);
}

// API calls
async function apiCall(endpoint, options = {}) {
    try {
        const response = await fetch(endpoint, {
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            ...options
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('API call failed:', error);
        showToast(`API Error: ${error.message}`, 'error');
        throw error;
    }
}

// Dashboard
async function loadDashboard() {
    try {
        dashboardData = await apiCall('/api/dashboard');
        updateDashboardStats();
        updateDashboardCharts();
    } catch (error) {
        console.error('Failed to load dashboard:', error);
    }
}

function updateDashboardStats() {
    if (!dashboardData) return;
    
    // Debug: Log dashboard data to see what fields are available
    console.log('Dashboard data received:', dashboardData);
    
    const moviesTotal = dashboardData.movies_total || 0;
    const moviesWithDates = dashboardData.movies_with_dates || 0;
    const moviesWithoutDates = dashboardData.movies_without_dates || (moviesTotal - moviesWithDates);
    
    const episodesTotal = dashboardData.episodes_total || 0;
    const episodesWithDates = dashboardData.episodes_with_dates || 0;
    const episodesWithoutDates = dashboardData.episodes_without_dates || (episodesTotal - episodesWithDates);
    
    document.getElementById('movies-total').textContent = moviesTotal;
    document.getElementById('movies-with-dates').textContent = `${moviesWithDates} with dates, ${moviesWithoutDates} without`;
    
    document.getElementById('series-total').textContent = dashboardData.series_count || 0;
    document.getElementById('episodes-total').textContent = `${episodesTotal} episodes (${episodesWithDates} with dates, ${episodesWithoutDates} without)`;
    
    const missingTotal = moviesWithoutDates + episodesWithoutDates;
    document.getElementById('missing-dates-total').textContent = missingTotal;
    
    const noValidTotal = (dashboardData.movies_no_valid_source || 0) + (dashboardData.episodes_no_valid_source || 0);
    document.getElementById('no-valid-source-total').textContent = `${moviesWithoutDates} movies, ${episodesWithoutDates} episodes without dates`;
    
    document.getElementById('recent-activity').textContent = dashboardData.recent_activity_count || 0;
}

function updateDashboardCharts() {
    if (!dashboardData) return;
    
    // Movie sources chart
    const movieChart = document.getElementById('movie-sources-chart');
    if (dashboardData.movie_sources && dashboardData.movie_sources.length > 0) {
        movieChart.innerHTML = createSimpleChart(dashboardData.movie_sources);
    } else {
        movieChart.innerHTML = '<p>No movie source data available</p>';
    }
    
    // Episode sources chart
    const episodeChart = document.getElementById('episode-sources-chart');
    if (dashboardData.episode_sources && dashboardData.episode_sources.length > 0) {
        episodeChart.innerHTML = createSimpleChart(dashboardData.episode_sources);
    } else {
        episodeChart.innerHTML = '<p>No episode source data available</p>';
    }
}

function createSimpleChart(data) {
    const total = data.reduce((sum, item) => sum + item.count, 0);
    let html = '<div class="simple-chart">';
    
    data.forEach((item, index) => {
        const percentage = ((item.count / total) * 100).toFixed(1);
        const color = getChartColor(index);
        html += `
            <div class="chart-item" style="background-color: ${color}20; border-left: 4px solid ${color};">
                <span class="chart-label">${item.source}</span>
                <span class="chart-value">${item.count} (${percentage}%)</span>
            </div>
        `;
    });
    
    html += '</div>';
    return html;
}

function getChartColor(index) {
    const colors = ['#007bff', '#28a745', '#ffc107', '#dc3545', '#6c757d', '#17a2b8', '#6f42c1'];
    return colors[index % colors.length];
}

// Movies
async function loadMovies(page = 1) {
    // Ensure page is a valid number
    if (isNaN(page) || page < 1) {
        page = 1;
    }
    
    const search = document.getElementById('movies-search').value;
    const imdbSearch = document.getElementById('movies-imdb-search').value;
    const hasDate = document.getElementById('movies-filter-date').value;
    const sourceFilter = document.getElementById('movies-filter-source').value;
    
    const skip = (page - 1) * 100;
    console.log(`DEBUG: loadMovies called with page=${page}, calculated skip=${skip}`);
    
    const params = new URLSearchParams({
        skip: skip,
        limit: 100
    });
    
    if (search) params.append('search', search);
    if (imdbSearch) params.append('imdb_search', imdbSearch);
    if (hasDate) params.append('has_date', hasDate);
    if (sourceFilter) params.append('source_filter', sourceFilter);
    
    try {
        const data = await apiCall(`/api/movies?${params}`);
        updateMoviesTable(data);
        updateMoviesPagination(data);
        updateMoviesSourceFilter(data);
        currentMoviesPage = (isNaN(page) || page < 1) ? 1 : page;
    } catch (error) {
        console.error('Failed to load movies:', error);
    }
}

function updateMoviesTable(data) {
    const tbody = document.getElementById('movies-tbody');
    
    if (!data.movies || data.movies.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center">No movies found</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.movies.map(movie => {
        const dateadded = movie.dateadded ? formatDateTime(movie.dateadded) : '';
        const hasVideoBadge = movie.has_video_file ? 
            '<span class="badge badge-success">Yes</span>' : 
            '<span class="badge badge-secondary">No</span>';
        
        // Determine date type based on source and dates
        let dateType = 'Unknown';
        let dateTypeBadge = 'badge-secondary';
        
        if (movie.source === 'digital_release') {
            dateType = 'Digital Release';
            dateTypeBadge = 'badge-success';
        } else if (movie.source && movie.source.includes('radarr') && movie.source.includes('import')) {
            dateType = 'Radarr Import';
            dateTypeBadge = 'badge-warning';
        } else if (movie.source === 'manual') {
            dateType = 'Manual';
            dateTypeBadge = 'badge-info';
        } else if (movie.source === 'nfo_file_existing') {
            dateType = 'Existing NFO';
            dateTypeBadge = 'badge-secondary';
        } else if (movie.source === 'no_valid_date_source') {
            dateType = 'No Valid Source';
            dateTypeBadge = 'badge-danger';
        } else if (movie.source && movie.source.toLowerCase().includes('tmdb:theatrical')) {
            dateType = 'TMDB Theatrical';
            dateTypeBadge = 'badge-primary';
        } else if (movie.source && movie.source.toLowerCase().includes('tmdb:digital')) {
            dateType = 'TMDB Digital';
            dateTypeBadge = 'badge-primary';
        } else if (movie.source && movie.source.toLowerCase().includes('tmdb:physical')) {
            dateType = 'TMDB Physical';
            dateTypeBadge = 'badge-primary';
        } else if (movie.source && movie.source.toLowerCase().includes('tmdb:')) {
            dateType = 'TMDB Release';
            dateTypeBadge = 'badge-primary';
        } else if (movie.source && movie.source.toLowerCase().includes('omdb:')) {
            dateType = 'OMDb Release';
            dateTypeBadge = 'badge-info';
        } else if (movie.source && movie.source.toLowerCase().includes('webhook:')) {
            dateType = 'Webhook/API';
            dateTypeBadge = 'badge-warning';
        }
        
        return `
            <tr>
                <td>${escapeHtml(movie.title)}</td>
                <td><code>${movie.imdb_id}</code></td>
                <td>${movie.released || '-'}</td>
                <td>${dateadded || '-'}</td>
                <td><span class="badge badge-secondary">${movie.source_description || movie.source || 'Unknown'}</span></td>
                <td><span class="badge ${dateTypeBadge}">${dateType}</span></td>
                <td>${hasVideoBadge}</td>
                <td>
                    <button class="btn btn-sm btn-primary" onclick="editMovie('${movie.imdb_id}', '${dateadded}', '${movie.source || ''}')">
                        <i class="fas fa-edit"></i> Edit
                    </button>
                    <button class="btn btn-sm btn-secondary" onclick="debugMovie('${movie.imdb_id}')" title="Debug Data">
                        <i class="fas fa-bug"></i>
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="deleteMovie('${movie.imdb_id}')" style="margin-left: 5px;" title="Delete Movie">
                        <i class="fas fa-trash"></i> Delete
                    </button>
                </td>
            </tr>
        `;
    }).join('');
}

function updateMoviesPagination(data) {
    const pagination = document.getElementById('movies-pagination');
    
    if (data.pages <= 1) {
        pagination.innerHTML = '';
        return;
    }
    
    let html = '';
    
    if (data.has_prev) {
        html += `<button class="btn btn-secondary btn-sm" onclick="loadMovies(${data.page - 1})">
            <i class="fas fa-chevron-left"></i> Previous
        </button>`;
    }
    
    html += `<span class="page-info">Page ${data.page} of ${data.pages}</span>`;
    
    if (data.has_next) {
        html += `<button class="btn btn-secondary btn-sm" onclick="loadMovies(${data.page + 1})">
            Next <i class="fas fa-chevron-right"></i>
        </button>`;
    }
    
    pagination.innerHTML = html;
}

function updateMoviesSourceFilter(data) {
    // This would be populated from dashboard data
    if (dashboardData && dashboardData.movie_sources) {
        const select = document.getElementById('movies-filter-source');
        const currentValue = select.value;
        
        select.innerHTML = '<option value="">All Sources</option>';
        dashboardData.movie_sources.forEach(source => {
            select.innerHTML += `<option value="${source.source}">${source.source} (${source.count})</option>`;
        });
        
        select.value = currentValue;
    }
}

async function loadSeriesSources() {
    try {
        const data = await apiCall('/api/series/sources');
        const select = document.getElementById('series-filter-source');
        const currentValue = select.value;
        
        select.innerHTML = '<option value="">All Sources</option>';
        data.sources.forEach(source => {
            select.innerHTML += `<option value="${source}">${source}</option>`;
        });
        
        select.value = currentValue;
    } catch (error) {
        console.error('Failed to load series sources:', error);
    }
}

function refreshMovies() {
    loadMovies(isNaN(currentMoviesPage) ? 1 : currentMoviesPage);
}

// TV Series
async function loadSeries(page = 1) {
    // Ensure page is a valid number
    if (isNaN(page) || page < 1) {
        page = 1;
    }
    
    const search = document.getElementById('series-search').value;
    const imdbSearch = document.getElementById('series-imdb-search').value;
    const dateFilter = document.getElementById('series-filter-date').value;
    const sourceFilter = document.getElementById('series-filter-source').value;
    
    const skip = (page - 1) * 50;
    console.log(`DEBUG: loadSeries called with page=${page}, calculated skip=${skip}`);
    
    const params = new URLSearchParams({
        skip: skip,
        limit: 50
    });
    
    if (search) params.append('search', search);
    if (imdbSearch) params.append('imdb_search', imdbSearch);
    if (dateFilter) params.append('date_filter', dateFilter);
    if (sourceFilter) params.append('source_filter', sourceFilter);
    
    try {
        const data = await apiCall(`/api/series?${params}`);
        updateSeriesTable(data);
        updateSeriesPagination(data);
        currentSeriesPage = (isNaN(page) || page < 1) ? 1 : page;
    } catch (error) {
        console.error('Failed to load series:', error);
    }
}

function updateSeriesTable(data) {
    const tbody = document.getElementById('series-tbody');
    
    if (!data.series || data.series.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center">No series found</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.series.map(series => {
        const progressPercent = series.total_episodes > 0 ? 
            ((series.episodes_with_dates / series.total_episodes) * 100).toFixed(1) : 0;
        
        return `
            <tr>
                <td>${escapeHtml(series.title)}</td>
                <td><code>${series.imdb_id}</code></td>
                <td>${series.total_episodes}</td>
                <td>
                    ${series.episodes_with_dates}
                    <small class="text-muted">(${progressPercent}%)</small>
                </td>
                <td>${series.episodes_with_video}</td>
                <td>
                    <button class="btn btn-sm btn-primary" onclick="viewSeriesEpisodes('${series.imdb_id}')">
                        <i class="fas fa-list"></i> Episodes
                    </button>
                </td>
            </tr>
        `;
    }).join('');
}

function updateSeriesPagination(data) {
    const pagination = document.getElementById('series-pagination');
    
    if (data.pages <= 1) {
        pagination.innerHTML = '';
        return;
    }
    
    let html = '';
    
    if (data.has_prev) {
        html += `<button class="btn btn-secondary btn-sm" onclick="loadSeries(${data.page - 1})">
            <i class="fas fa-chevron-left"></i> Previous
        </button>`;
    }
    
    html += `<span class="page-info">Page ${data.page} of ${data.pages}</span>`;
    
    if (data.has_next) {
        html += `<button class="btn btn-secondary btn-sm" onclick="loadSeries(${data.page + 1})">
            Next <i class="fas fa-chevron-right"></i>
        </button>`;
    }
    
    pagination.innerHTML = html;
}

function refreshSeries() {
    loadSeries(isNaN(currentSeriesPage) ? 1 : currentSeriesPage);
}

async function viewSeriesEpisodes(imdbId) {
    try {
        const data = await apiCall(`/api/series/${imdbId}/episodes`);
        showEpisodesModal(data);
    } catch (error) {
        console.error('Failed to load episodes:', error);
    }
}

function showEpisodesModal(data) {
    // Calculate statistics
    const totalEpisodes = data.episodes.length;
    const episodesWithDates = data.episodes.filter(ep => ep.dateadded && ep.dateadded.trim() !== '').length;
    const episodesWithoutDates = totalEpisodes - episodesWithDates;
    const episodesWithVideo = data.episodes.filter(ep => ep.has_video_file).length;
    
    const modalHtml = `
        <div class="modal active" id="episodes-modal">
            <div class="modal-content" style="max-width: 900px;">
                <div class="modal-header">
                    <h3>${escapeHtml(data.series.title)} - Episodes</h3>
                    <button class="modal-close" onclick="closeEpisodesModal()">&times;</button>
                </div>
                <div class="modal-body">
                    <div class="episode-stats" style="display: flex; gap: 20px; margin-bottom: 20px; padding: 15px; background: #f8f9fa; border-radius: 5px;">
                        <div><strong>Total Episodes:</strong> ${totalEpisodes}</div>
                        <div><strong>With Dates:</strong> ${episodesWithDates}</div>
                        <div style="color: #dc3545;"><strong>Missing Dates:</strong> ${episodesWithoutDates}</div>
                        <div><strong>With Video:</strong> ${episodesWithVideo}</div>
                    </div>
                    
                    <div class="episode-filters" style="margin-bottom: 15px;">
                        <label style="margin-right: 15px;">
                            <input type="radio" name="episode-filter" value="all" checked onchange="filterEpisodes('all')"> Show All
                        </label>
                        <label style="margin-right: 15px;">
                            <input type="radio" name="episode-filter" value="missing" onchange="filterEpisodes('missing')"> Missing Dates Only
                        </label>
                        <label>
                            <input type="radio" name="episode-filter" value="has-dates" onchange="filterEpisodes('has-dates')"> With Dates Only
                        </label>
                    </div>
                    
                    <div class="table-container">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>Episode</th>
                                    <th>Aired</th>
                                    <th>Date Added</th>
                                    <th>Source</th>
                                    <th>Video</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody id="episodes-table-body">
                                ${data.episodes.map(episode => {
                                    const dateadded = episode.dateadded ? formatDateTime(episode.dateadded) : '';
                                    const hasVideoBadge = episode.has_video_file ? 
                                        '<span class="badge badge-success">Yes</span>' : 
                                        '<span class="badge badge-secondary">No</span>';
                                    
                                    const missingDate = !episode.dateadded || episode.dateadded.trim() === '';
                                    const rowClass = missingDate ? 'missing-date-row' : '';
                                    const dateCell = missingDate ? 
                                        '<td style="background-color: #ffebee; color: #c62828;"><strong>MISSING</strong></td>' : 
                                        `<td>${dateadded}</td>`;
                                    
                                    return `
                                        <tr class="${rowClass}" data-has-date="${!missingDate}">
                                            <td>S${episode.season.toString().padStart(2, '0')}E${episode.episode.toString().padStart(2, '0')}</td>
                                            <td>${episode.aired || '-'}</td>
                                            ${dateCell}
                                            <td><span class="badge badge-secondary">${episode.source_description || episode.source || 'Unknown'}</span></td>
                                            <td>${hasVideoBadge}</td>
                                            <td>
                                                <button class="btn btn-sm btn-primary" onclick="editEpisode('${data.series.imdb_id}', ${episode.season}, ${episode.episode}, '${dateadded}', '${episode.source || ''}')">
                                                    <i class="fas fa-edit"></i> Edit
                                                </button>
                                                <button class="btn btn-sm btn-danger" onclick="deleteEpisode('${data.series.imdb_id}', ${episode.season}, ${episode.episode})" style="margin-left: 5px;">
                                                    <i class="fas fa-trash"></i> Delete
                                                </button>
                                            </td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
}

function filterEpisodes(filterType) {
    const rows = document.querySelectorAll('#episodes-table-body tr');
    
    rows.forEach(row => {
        const hasDate = row.getAttribute('data-has-date') === 'true';
        let shouldShow = true;
        
        switch (filterType) {
            case 'missing':
                shouldShow = !hasDate;
                break;
            case 'has-dates':
                shouldShow = hasDate;
                break;
            case 'all':
            default:
                shouldShow = true;
                break;
        }
        
        row.style.display = shouldShow ? '' : 'none';
    });
}

function closeEpisodesModal() {
    const modal = document.getElementById('episodes-modal');
    if (modal) {
        modal.remove();
    }
}

// Reports
async function loadReport() {
    try {
        const data = await apiCall('/api/reports/missing-dates');
        updateReportSummary(data.summary);
        updateReportTables(data);
    } catch (error) {
        console.error('Failed to load report:', error);
    }
}

function updateReportSummary(summary) {
    document.getElementById('report-movies-with').textContent = summary.movies_with_dates;
    document.getElementById('report-movies-missing').textContent = summary.movies_missing_dates;
    document.getElementById('report-episodes-with').textContent = summary.episodes_with_dates;
    document.getElementById('report-episodes-missing').textContent = summary.episodes_missing_dates;
}

function updateReportTables(data) {
    // Movies missing dates
    const moviesTbody = document.getElementById('report-movies-tbody');
    if (data.movies_missing.length === 0) {
        moviesTbody.innerHTML = '<tr><td colspan="5" class="text-center">No movies missing dates</td></tr>';
    } else {
        moviesTbody.innerHTML = data.movies_missing.map(movie => `
            <tr>
                <td>${escapeHtml(movie.title)}</td>
                <td><code>${movie.imdb_id}</code></td>
                <td>${movie.released || '-'}</td>
                <td><span class="badge badge-warning">${movie.source_description || movie.source || 'Unknown'}</span></td>
                <td>
                    <button class="btn btn-sm btn-success" onclick="smartFixMovie('${movie.imdb_id}')">
                        <i class="fas fa-magic"></i> Smart Fix
                    </button>
                </td>
            </tr>
        `).join('');
    }
    
    // Episodes missing dates
    const episodesTbody = document.getElementById('report-episodes-tbody');
    if (data.episodes_missing.length === 0) {
        episodesTbody.innerHTML = '<tr><td colspan="6" class="text-center">No episodes missing dates</td></tr>';
    } else {
        episodesTbody.innerHTML = data.episodes_missing.map(episode => `
            <tr>
                <td>${escapeHtml(episode.series_title)}</td>
                <td>S${episode.season.toString().padStart(2, '0')}E${episode.episode.toString().padStart(2, '0')}</td>
                <td><code>${episode.imdb_id}</code></td>
                <td>${episode.aired || '-'}</td>
                <td><span class="badge badge-warning">${episode.source_description || episode.source || 'Unknown'}</span></td>
                <td>
                    <button class="btn btn-sm btn-success" onclick="smartFixEpisode('${episode.imdb_id}', ${episode.season}, ${episode.episode})">
                        <i class="fas fa-magic"></i> Smart Fix
                    </button>
                </td>
            </tr>
        `).join('');
    }
}

function refreshReport() {
    loadReport();
}

// Smart fix functions
async function smartFixMovie(imdbId) {
    try {
        console.log('🔍 SMART FIX: Loading options for movie', imdbId);
        const options = await apiCall(`/api/movies/${imdbId}/date-options`);
        console.log('🔍 SMART FIX: Received options:', options);
        showSmartFixModal('movie', options);
    } catch (error) {
        console.error('Failed to load movie options:', error);
        showToast('Failed to load movie options', 'error');
    }
}

async function smartFixEpisode(imdbId, season, episode) {
    try {
        const options = await apiCall(`/api/episodes/${imdbId}/${season}/${episode}/date-options`);
        showSmartFixModal('episode', options);
    } catch (error) {
        console.error('Failed to load episode options:', error);
        showToast('Failed to load episode options', 'error');
    }
}

function showSmartFixModal(mediaType, options) {
    console.log('🔍 SMART FIX: Showing modal for', mediaType, 'with options:', options);
    
    const modal = document.getElementById('smart-fix-modal');
    const title = document.getElementById('smart-fix-title');
    const content = document.getElementById('smart-fix-content');
    
    if (!modal || !title || !content) {
        console.error('❌ SMART FIX: Modal elements not found!', {modal, title, content});
        alert('Smart Fix modal not found - check console for details');
        return;
    }
    
    console.log('✅ SMART FIX: Modal elements found, proceeding to show Smart Fix modal');
    
    if (mediaType === 'movie') {
        title.textContent = `Fix Date for Movie: ${options.imdb_id}`;
    } else {
        title.textContent = `Fix Date for Episode: ${options.imdb_id} S${options.season.toString().padStart(2, '0')}E${options.episode.toString().padStart(2, '0')}`;
    }
    
    // Build options HTML
    let optionsHtml = '<div class="smart-fix-options">';
    
    options.options.forEach((option, index) => {
        const isChecked = index === 0 ? 'checked' : '';
        const dateInput = option.type === 'manual' ? 
            `<input type="datetime-local" id="manual-date-${index}" class="manual-date-input" style="margin-top: 0.5rem;">` : '';
        
        optionsHtml += `
            <div class="option-card">
                <label class="option-label">
                    <input type="radio" name="date-option" value="${index}" ${isChecked}>
                    <div class="option-content">
                        <h4>${option.label}</h4>
                        <p>${option.description}</p>
                        ${option.date ? `<small><strong>Date:</strong> ${formatDateTime(option.date)}</small>` : ''}
                        ${dateInput}
                    </div>
                </label>
            </div>
        `;
    });
    
    optionsHtml += '</div>';
    
    optionsHtml += `
        <div class="form-actions">
            <button type="button" class="btn btn-secondary" onclick="closeSmartFixModal()">Cancel</button>
            <button type="button" class="btn btn-success" onclick="applySmartFix('${mediaType}', ${JSON.stringify(options).replace(/'/g, "&apos;")})">
                <i class="fas fa-magic"></i> Apply Fix
            </button>
        </div>
    `;
    
    content.innerHTML = optionsHtml;
    modal.classList.add('active');
}

function closeSmartFixModal() {
    document.getElementById('smart-fix-modal').classList.remove('active');
}

async function applySmartFix(mediaType, options) {
    const selectedRadio = document.querySelector('input[name="date-option"]:checked');
    if (!selectedRadio) {
        showToast('Please select a date option', 'warning');
        return;
    }
    
    const selectedIndex = selectedRadio.value;
    const selectedOption = options.options[selectedIndex];
    
    let dateadded = selectedOption.date;
    let source = selectedOption.source;
    
    // Handle manual date entry
    if (selectedOption.type === 'manual') {
        const manualDateInput = document.getElementById(`manual-date-${selectedIndex}`);
        if (manualDateInput && manualDateInput.value) {
            try {
                dateadded = new Date(manualDateInput.value).toISOString();
            } catch (e) {
                showToast('Invalid date format', 'error');
                return;
            }
        } else {
            showToast('Please enter a date for manual option', 'warning');
            return;
        }
    } else if (dateadded) {
        // Fix date format for non-manual options
        try {
            let dateValue = dateadded;
            
            // Handle timezone offsets
            if (dateValue.includes('+00:00')) {
                dateValue = dateValue.replace('+00:00', 'Z');
            }
            
            const date = new Date(dateValue);
            if (isNaN(date.getTime())) {
                showToast('Invalid date from server', 'error');
                return;
            }
            dateadded = date.toISOString();
        } catch (e) {
            console.error('Date conversion error:', e, dateadded);
            showToast('Date conversion error', 'error');
            return;
        }
    }
    
    // Debug logging
    console.log('🔍 SMART FIX DEBUG:', {
        mediaType,
        imdb_id: options.imdb_id,
        selectedOption,
        dateadded,
        source,
        originalDate: selectedOption.date
    });
    
    try {
        if (mediaType === 'movie') {
            await updateMovieDate(options.imdb_id, dateadded, source);
        } else {
            await updateEpisodeDate(options.imdb_id, options.season, options.episode, dateadded, source);
        }
        closeSmartFixModal();
    } catch (error) {
        console.error('Smart fix failed:', error);
        showToast('Smart fix failed: ' + error.message, 'error');
    }
}

// Tools
async function loadDetailedStats() {
    try {
        const data = await apiCall('/api/dashboard');
        const statsHtml = `
            <div class="stats-grid">
                <div class="stat-row">
                    <strong>Database Size:</strong> ${data.database_size_mb} MB
                </div>
                <div class="stat-row">
                    <strong>Total Movies:</strong> ${data.movies_total} (${data.movies_with_video} with video files)
                </div>
                <div class="stat-row">
                    <strong>Movies with Dates:</strong> ${data.movies_with_dates} (${((data.movies_with_dates / data.movies_total) * 100).toFixed(1)}%)
                </div>
                <div class="stat-row">
                    <strong>Total Series:</strong> ${data.series_count}
                </div>
                <div class="stat-row">
                    <strong>Total Episodes:</strong> ${data.episodes_total} (${data.episodes_with_video} with video files)
                </div>
                <div class="stat-row">
                    <strong>Episodes with Dates:</strong> ${data.episodes_with_dates} (${((data.episodes_with_dates / data.episodes_total) * 100).toFixed(1)}%)
                </div>
                <div class="stat-row">
                    <strong>Processing History:</strong> ${data.processing_history_count} events
                </div>
            </div>
        `;
        document.getElementById('detailed-stats').innerHTML = statsHtml;
    } catch (error) {
        console.error('Failed to load detailed stats:', error);
    }
}

async function handleBulkUpdate(event) {
    event.preventDefault();
    
    const mediaType = document.getElementById('bulk-media-type').value;
    const oldSource = document.getElementById('bulk-old-source').value;
    const newSource = document.getElementById('bulk-new-source').value;
    
    if (!mediaType || !oldSource || !newSource) {
        showToast('Please fill in all fields', 'warning');
        return;
    }
    
    if (!confirm(`This will update all ${mediaType} with source "${oldSource}" to "${newSource}". Continue?`)) {
        return;
    }
    
    try {
        const result = await apiCall('/api/bulk/update-source', {
            method: 'POST',
            body: JSON.stringify({
                media_type: mediaType,
                old_source: oldSource,
                new_source: newSource
            })
        });
        
        showToast(result.message, 'success');
        
        // Reset form
        document.getElementById('bulk-update-form').reset();
        
        // Refresh current tab
        if (currentTab === 'movies') loadMovies(currentMoviesPage);
        if (currentTab === 'tv') loadSeries(currentSeriesPage);
        if (currentTab === 'reports') loadReport();
        if (currentTab === 'dashboard') loadDashboard();
        
    } catch (error) {
        console.error('Bulk update failed:', error);
    }
}

// Edit modal functions
async function editMovie(imdbId, dateadded, source) {
    try {
        // Load movie options to populate available dates
        const options = await apiCall(`/api/movies/${imdbId}/date-options`);
        showEnhancedEditModal('movie', options, dateadded, source);
    } catch (error) {
        console.error('Failed to load movie options for edit:', error);
        // Fallback to basic edit modal
        showBasicEditModal('movie', imdbId, dateadded, source);
    }
}

function showEnhancedEditModal(mediaType, options, currentDateadded, currentSource) {
    const modal = document.getElementById('edit-modal');
    const title = document.getElementById('modal-title');
    const modalBody = document.querySelector('#edit-modal .modal-body');
    
    if (mediaType === 'movie') {
        title.textContent = `Edit Movie: ${options.imdb_id}`;
    } else {
        title.textContent = `Edit Episode: ${options.imdb_id} S${options.season.toString().padStart(2, '0')}E${options.episode.toString().padStart(2, '0')}`;
    }
    
    // Build enhanced edit form with date options
    let formHtml = `
        <input type="hidden" id="edit-imdb-id" value="${options.imdb_id}">
        <input type="hidden" id="edit-media-type" value="${mediaType}">
        ${mediaType === 'episode' ? `
            <input type="hidden" id="edit-season" value="${options.season}">
            <input type="hidden" id="edit-episode" value="${options.episode}">
        ` : `
            <input type="hidden" id="edit-season" value="">
            <input type="hidden" id="edit-episode" value="">
        `}
        
        <div class="form-group">
            <label>Choose Date Source:</label>
            <div class="date-options">
    `;
    
    // Add available date options
    options.options.forEach((option, index) => {
        const isSelected = option.source === currentSource ? 'checked' : '';
        const optionId = `date-option-${index}`;
        
        formHtml += `
            <div class="date-option-card">
                <label class="date-option-label">
                    <input type="radio" name="edit-date-option" value="${index}" ${isSelected} 
                           onchange="updateEditDateFromOption(${index}, ${JSON.stringify(option).replace(/"/g, '&quot;')})">
                    <div class="date-option-content">
                        <h4>${option.label}</h4>
                        <p>${option.description}</p>
                        ${option.date ? `<small><strong>Date:</strong> ${formatDateTime(option.date)}</small>` : ''}
                    </div>
                </label>
            </div>
        `;
    });
    
    formHtml += `
            </div>
        </div>
        
        <div class="form-group">
            <label for="edit-dateadded">Date Added:</label>
            <input type="datetime-local" id="edit-dateadded" required>
            <small>Adjust the date/time as needed</small>
        </div>
        
        <div class="form-group">
            <label for="edit-source">Source:</label>
            <select id="edit-source" required>
                <option value="manual">Manual</option>
                <option value="airdate">Air Date</option>
                <option value="digital_release">Digital Release</option>
                <option value="radarr:db.history.import">Radarr Import</option>
                <option value="sonarr:history.import">Sonarr Import</option>
                <option value="no_valid_date_source">No Valid Source</option>
            </select>
        </div>
        
        <div class="form-actions">
            <button type="button" class="btn btn-secondary" onclick="closeModal()">Cancel</button>
            <button type="submit" class="btn btn-primary" onclick="handleEnhancedEditSubmit(event)">Save Changes</button>
        </div>
    `;
    
    modalBody.innerHTML = formHtml;
    
    
    // Set current values
    if (currentDateadded && currentDateadded !== '-') {
        try {
            const date = new Date(currentDateadded);
            document.getElementById('edit-dateadded').value = date.toISOString().slice(0, 16);
        } catch (e) {
            document.getElementById('edit-dateadded').value = '';
        }
    }
    
    document.getElementById('edit-source').value = currentSource || 'manual';
    
    // Store options for later use
    modal.dataset.options = JSON.stringify(options);
    
    modal.classList.add('active');
}

function showBasicEditModal(mediaType, imdbId, dateadded, source) {
    // Fallback to original basic edit modal
    document.getElementById('modal-title').textContent = `Edit ${mediaType}: ${imdbId}`;
    document.getElementById('edit-imdb-id').value = imdbId;
    document.getElementById('edit-media-type').value = mediaType;
    
    if (dateadded && dateadded !== '-') {
        try {
            const date = new Date(dateadded);
            document.getElementById('edit-dateadded').value = date.toISOString().slice(0, 16);
        } catch (e) {
            document.getElementById('edit-dateadded').value = '';
        }
    } else {
        document.getElementById('edit-dateadded').value = '';
    }
    
    document.getElementById('edit-source').value = source || 'manual';
    document.getElementById('edit-modal').classList.add('active');
}

function updateEditDateFromOption(optionIndex, option) {
    const dateInput = document.getElementById('edit-dateadded');
    const sourceSelect = document.getElementById('edit-source');
    
    if (option.date) {
        // Convert to datetime-local format with better date parsing
        try {
            let dateValue = option.date;
            
            // Handle timezone offsets by converting to local time
            if (dateValue.includes('+00:00') || dateValue.includes('Z')) {
                dateValue = dateValue.replace('+00:00', 'Z');
            }
            
            const date = new Date(dateValue);
            if (isNaN(date.getTime())) {
                console.error('Invalid date:', dateValue);
                dateInput.value = '';
            } else {
                // Convert to local datetime-local format
                const localDateTime = new Date(date.getTime() - (date.getTimezoneOffset() * 60000));
                dateInput.value = localDateTime.toISOString().slice(0, 16);
            }
        } catch (e) {
            console.error('Date parsing error:', e, option.date);
            dateInput.value = '';
        }
    } else {
        // Manual option - clear the date for user input
        dateInput.value = '';
    }
    
    sourceSelect.value = option.source;
}

async function handleEnhancedEditSubmit(event) {
    event.preventDefault();
    
    const modal = document.getElementById('edit-modal');
    const options = JSON.parse(modal.dataset.options);
    const imdbId = options.imdb_id;
    const mediaType = document.getElementById('edit-media-type').value;
    const dateadded = document.getElementById('edit-dateadded').value;
    const source = document.getElementById('edit-source').value;
    
    if (!dateadded) {
        showToast('Please enter a date', 'warning');
        return;
    }
    
    // Convert datetime-local to ISO string with error handling
    let isoDateadded = null;
    try {
        isoDateadded = new Date(dateadded).toISOString();
    } catch (e) {
        showToast('Invalid date format', 'error');
        return;
    }
    
    try {
        if (mediaType === 'movie') {
            await updateMovieDate(imdbId, isoDateadded, source);
        } else {
            await updateEpisodeDate(imdbId, options.season, options.episode, isoDateadded, source);
        }
        
        closeModal();
    } catch (error) {
        console.error('Enhanced edit failed:', error);
        showToast('Update failed: ' + error.message, 'error');
    }
}

async function editEpisode(imdbId, season, episode, dateadded, source) {
    try {
        // Load episode options to populate available dates
        const options = await apiCall(`/api/episodes/${imdbId}/${season}/${episode}/date-options`);
        showEnhancedEditModal('episode', options, dateadded, source);
    } catch (error) {
        console.error('Failed to load episode options for edit:', error);
        // Fallback to basic edit modal
        showBasicEditModal('episode', imdbId, dateadded, source, season, episode);
    }
}

function closeModal() {
    document.getElementById('edit-modal').classList.remove('active');
}

async function handleEditSubmit(event) {
    event.preventDefault();
    
    const imdbId = document.getElementById('edit-imdb-id').value;
    const mediaType = document.getElementById('edit-media-type').value;
    const season = document.getElementById('edit-season').value;
    const episode = document.getElementById('edit-episode').value;
    const dateadded = document.getElementById('edit-dateadded').value;
    const source = document.getElementById('edit-source').value;
    
    // Convert datetime-local to ISO string
    const isoDateadded = dateadded ? new Date(dateadded).toISOString() : null;
    
    try {
        if (mediaType === 'movie') {
            await updateMovieDate(imdbId, isoDateadded, source);
        } else {
            await updateEpisodeDate(imdbId, parseInt(season), parseInt(episode), isoDateadded, source);
        }
        
        closeModal();
    } catch (error) {
        console.error('Update failed:', error);
    }
}

// Update functions
async function updateMovieDate(imdbId, dateadded, source) {
    try {
        const result = await apiCall(`/api/movies/${imdbId}`, {
            method: 'PUT',
            body: JSON.stringify({
                dateadded: dateadded,
                source: source
            })
        });
        
        showToast(result.message, 'success');
        
        // Refresh current view
        if (currentTab === 'movies') loadMovies(currentMoviesPage);
        if (currentTab === 'reports') loadReport();
        if (currentTab === 'dashboard') loadDashboard();
        
    } catch (error) {
        console.error('Movie update failed:', error);
    }
}

async function updateEpisodeDate(imdbId, season, episode, dateadded, source) {
    try {
        const result = await apiCall(`/api/episodes/${imdbId}/${season}/${episode}`, {
            method: 'PUT',
            body: JSON.stringify({
                dateadded: dateadded,
                source: source
            })
        });
        
        showToast(result.message, 'success');
        
        // Refresh current view
        if (currentTab === 'tv') loadSeries(currentSeriesPage);
        if (currentTab === 'reports') loadReport();
        if (currentTab === 'dashboard') loadDashboard();
        
        // Refresh episodes modal if open
        const episodesModal = document.getElementById('episodes-modal');
        if (episodesModal) {
            closeEpisodesModal();
            setTimeout(() => viewSeriesEpisodes(imdbId), 100);
        }
        
    } catch (error) {
        console.error('Episode update failed:', error);
    }
}

// Utility functions
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function formatDateTime(dateString) {
    try {
        const date = new Date(dateString);
        return date.toLocaleString();
    } catch (e) {
        return dateString;
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <div class="toast-content">
            <span>${escapeHtml(message)}</span>
        </div>
    `;
    
    container.appendChild(toast);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        if (toast.parentNode) {
            toast.parentNode.removeChild(toast);
        }
    }, 5000);
    
    // Remove on click
    toast.addEventListener('click', () => {
        if (toast.parentNode) {
            toast.parentNode.removeChild(toast);
        }
    });
}

// Debug function
async function debugMovie(imdbId) {
    try {
        const data = await apiCall(`/api/debug/movie/${imdbId}/raw`);
        
        const debugInfo = `
DEBUG INFO for ${imdbId}:

Raw Database Data:
- imdb_id: ${data.raw_data.imdb_id}
- path: ${data.raw_data.path}
- released: ${data.raw_data.released}
- dateadded: ${data.raw_data.dateadded}
- source: ${data.raw_data.source}
- has_video_file: ${data.raw_data.has_video_file}
- last_updated: ${data.raw_data.last_updated}

Analysis:
- Movie Released: ${data.raw_data.released || 'Not set'}
- Library Import Date: ${data.raw_data.dateadded || 'Not set'}
- Date Source: ${data.raw_data.source_description || data.raw_data.source || 'Unknown'}
        `;
        
        alert(debugInfo);
        console.log('🔍 Debug data for', imdbId, data);
        
    } catch (error) {
        console.error('Debug failed:', error);
        showToast('Debug failed: ' + error.message, 'error');
    }
}

// Episode deletion functionality
async function deleteEpisode(imdbId, season, episode) {
    const episodeStr = `S${season.toString().padStart(2, '0')}E${episode.toString().padStart(2, '0')}`;
    
    // Confirmation dialog
    if (!confirm(`⚠️ Delete Episode ${episodeStr}?\n\nThis will permanently remove the episode from the database.\n\nAre you sure you want to continue?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/database/episode/${imdbId}/${season}/${episode}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const result = await response.json();
        
        if (response.ok && result.success) {
            showToast(`✅ Episode ${episodeStr} deleted successfully`, 'success');
            
            // Remove the row from the table
            const rows = document.querySelectorAll('#episodes-table-body tr');
            rows.forEach(row => {
                const episodeCell = row.querySelector('td:first-child');
                if (episodeCell && episodeCell.textContent === episodeStr) {
                    row.remove();
                }
            });
            
            // Update episode counts in modal header
            updateEpisodeModalCounts();
            
        } else {
            const errorMsg = result.message || result.error || 'Unknown error';
            showToast(`❌ Failed to delete episode: ${errorMsg}`, 'error');
        }
        
    } catch (error) {
        console.error('Delete episode failed:', error);
        showToast(`❌ Delete failed: ${error.message}`, 'error');
    }
}

// Movie deletion functionality
async function deleteMovie(imdbId) {
    // Confirmation dialog
    if (!confirm(`⚠️ Delete Movie?\n\nThis will permanently remove the movie (${imdbId}) from the database.\n\nAre you sure you want to continue?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/database/movie/${imdbId}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const result = await response.json();
        
        if (response.ok && result.success) {
            showToast(`✅ Movie deleted successfully`, 'success');
            
            // Refresh the movies table
            loadMovies(currentMoviesPage);
            
        } else {
            const errorMsg = result.message || result.error || 'Unknown error';
            showToast(`❌ Failed to delete movie: ${errorMsg}`, 'error');
        }
        
    } catch (error) {
        console.error('Delete movie failed:', error);
        showToast(`❌ Delete failed: ${error.message}`, 'error');
    }
}

// Update episode counts in modal after deletion
function updateEpisodeModalCounts() {
    const remainingRows = document.querySelectorAll('#episodes-table-body tr');
    const totalEpisodes = remainingRows.length;
    const episodesWithDates = Array.from(remainingRows).filter(row => 
        row.getAttribute('data-has-date') === 'true'
    ).length;
    const episodesWithoutDates = totalEpisodes - episodesWithDates;
    
    // Update the stats in the modal
    const statsDiv = document.querySelector('.episode-stats');
    if (statsDiv) {
        // Keep the existing "With Video" count by finding it
        const videoCountDiv = statsDiv.querySelector('div:nth-child(4)');
        const videoCountText = videoCountDiv ? videoCountDiv.innerHTML : '<div><strong>With Video:</strong> -</div>';
        
        statsDiv.innerHTML = `
            <div><strong>Total Episodes:</strong> ${totalEpisodes}</div>
            <div><strong>With Dates:</strong> ${episodesWithDates}</div>
            <div style="color: #dc3545;"><strong>Missing Dates:</strong> ${episodesWithoutDates}</div>
            ${videoCountText}
        `;
    }
}