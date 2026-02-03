/**
 * Lead Finder - Main Application JavaScript
 * 全国対応リード発掘ツール
 * Modern SaaS-style UI with micro-interactions
 */

// Global state
let allResults = [];
let csvPath = '';
let currentFilter = 'all';

/**
 * Document Ready
 */
$(document).ready(function() {
    // Form submit handler
    $('#searchForm').on('submit', handleSearch);

    // Download button
    $('#downloadBtn').on('click', handleDownload);

    // Cancel search button
    $('#cancelSearchBtn').on('click', cancelSearch);

    // Filter tab interactions
    initFilterTabs();

    // Card hover effects
    initCardInteractions();

    // Business type chip interactions
    initChipInteractions();

    // Preset button interactions
    initPresetButtons();

    // Range slider sync
    initRangeSliders();

    // AI verification toggle
    initAiToggle();

    // Smooth scroll for anchor links
    initSmoothScroll();

    // Load last search from localStorage
    loadLastSearch();

    // Add page load animation
    animatePageLoad();
});

/**
 * Animate page load
 */
function animatePageLoad() {
    // Stagger animation for hero stats
    $('.stat-card').each(function(index) {
        $(this).css({
            'opacity': '0',
            'transform': 'translateY(20px)'
        }).delay(100 * index).animate({
            'opacity': '1'
        }, 400, function() {
            $(this).css('transform', 'translateY(0)');
        });
    });
}

/**
 * Initialize filter tabs with smooth transitions
 */
function initFilterTabs() {
    $(document).on('click', '.filter-tab', function(e) {
        e.preventDefault();
        const $tab = $(this);
        const filter = $tab.data('filter');

        // Update active state with animation
        $('.filter-tab').removeClass('active');
        $tab.addClass('active');

        // Apply filter with fade animation
        filterResults(filter);
        currentFilter = filter;

        // Ripple effect
        createRipple(e, $tab);
    });
}

/**
 * Filter results with animation
 */
function filterResults(filter) {
    const $cards = $('.result-card');

    $cards.each(function(index) {
        const $card = $(this);
        const classification = $card.data('classification');
        const weakness = $card.data('weakness');

        let shouldShow = false;

        switch(filter) {
            case 'all':
                shouldShow = true;
                break;
            case 'solo':
                shouldShow = classification === 'solo';
                break;
            case 'small':
                shouldShow = classification === 'small';
                break;
            case 'weak':
                shouldShow = weakness >= 50;
                break;
        }

        if (shouldShow) {
            $card.removeClass('hidden').css({
                'opacity': '0',
                'transform': 'scale(0.95)'
            }).delay(index * 30).animate({
                'opacity': '1'
            }, 200, function() {
                $(this).css('transform', 'scale(1)');
            });
        } else {
            $card.addClass('hidden');
        }
    });
}

/**
 * Initialize card interactions
 */
function initCardInteractions() {
    // Hover lift effect for cards
    $(document).on('mouseenter', '.result-card', function() {
        $(this).addClass('card-hover');
    }).on('mouseleave', '.result-card', function() {
        $(this).removeClass('card-hover');
    });

    // Click to expand details (future feature placeholder)
    $(document).on('click', '.result-card', function(e) {
        if (!$(e.target).is('a')) {
            $(this).toggleClass('expanded');
        }
    });
}

/**
 * Initialize business type chip interactions
 */
function initChipInteractions() {
    $(document).on('change', 'input[name="business_type"]', function() {
        const $chip = $(this).closest('.chip-checkbox, .business-type-item');
        if ($(this).is(':checked')) {
            $chip.addClass('selected');
            pulseAnimation($chip);
        } else {
            $chip.removeClass('selected');
        }
        updateSelectedBusinessCount();
    });

    // Initial state
    $('input[name="business_type"]:checked').each(function() {
        $(this).closest('.chip-checkbox, .business-type-item').addClass('selected');
    });
    updateSelectedBusinessCount();

    // Select/Clear helpers
    $(document).on('click', '#selectAllBusinessTypes', function() {
        $('input[name="business_type"]').prop('checked', true).trigger('change');
    });
    $(document).on('click', '#clearBusinessTypes', function() {
        $('input[name="business_type"]').prop('checked', false).trigger('change');
    });

    // Per-category select all toggle
    $(document).on('click', '.select-category-all', function(e) {
        e.stopPropagation(); // Don't toggle collapse
        const categoryId = $(this).data('category');
        const $checkboxes = $(`input[name="business_type"][data-category="${categoryId}"]`);
        const allChecked = $checkboxes.length === $checkboxes.filter(':checked').length;
        $checkboxes.prop('checked', !allChecked).trigger('change');
    });
}

/**
 * Update selected business type count
 */
function updateSelectedBusinessCount() {
    const count = $('input[name="business_type"]:checked').length;
    const $counter = $('#selectedBusinessCount');
    if ($counter.length) {
        $counter.text(`${count}件選択中`);
    }
    // Update per-category counts
    $('.business-category').each(function() {
        const categoryId = $(this).data('category');
        const catCount = $(`input[name="business_type"][data-category="${categoryId}"]:checked`).length;
        const $badge = $(this).find('.category-count');
        if (catCount > 0) {
            $badge.text(catCount).show();
        } else {
            $badge.hide();
        }
    });
}

/**
 * Initialize preset buttons with active state
 */
function initPresetButtons() {
    $(document).on('click', '.btn-preset', function(e) {
        const $btn = $(this);
        const preset = $btn.data('preset');

        // Update active state
        $('.btn-preset').removeClass('active');
        $btn.addClass('active');

        // Apply preset with animation
        applyPreset(preset);

        // Ripple effect
        createRipple(e, $btn);

        // Pulse feedback
        pulseAnimation($btn);
    });
}

/**
 * Initialize range sliders with live value update
 */
function initRangeSliders() {
    // Min score slider
    $(document).on('input', '#minScoreRange', function() {
        const value = $(this).val();
        $('#minScoreInput').val(value);
        $('#minScoreValue').text(value);
    });

    // Weakness slider
    $(document).on('input', '#minWeakness', function() {
        const value = $(this).val();
        $('#minWeaknessValue').text(value);
    });

    // Sync input to slider
    $(document).on('input', '#minScoreInput', function() {
        const value = $(this).val();
        $('#minScoreRange').val(value);
        $('#minScoreValue').text(value);
    });
}

/**
 * Initialize AI verification toggle
 */
function initAiToggle() {
    $(document).on('change', '#useAiVerify', function() {
        const $container = $('#aiTopNContainer');
        if ($(this).is(':checked')) {
            $container.slideDown(200);
        } else {
            $container.slideUp(200);
        }
    });

    $(document).on('change', '#useAiRelevance', function() {
        const $container = $('#aiRelevanceTopNContainer');
        if ($(this).is(':checked')) {
            $container.slideDown(200);
        } else {
            $container.slideUp(200);
        }
    });

    // Initial state sync
    if ($('#useAiVerify').is(':checked')) {
        $('#aiTopNContainer').show();
    }
    if ($('#useAiRelevance').is(':checked')) {
        $('#aiRelevanceTopNContainer').show();
    }
}

/**
 * Initialize smooth scroll
 */
function initSmoothScroll() {
    $(document).on('click', 'a[href^="#"]', function(e) {
        const target = $(this.getAttribute('href'));
        if (target.length) {
            e.preventDefault();
            $('html, body').animate({
                scrollTop: target.offset().top - 80
            }, 500, 'swing');
        }
    });
}

/**
 * Create ripple effect on element
 */
function createRipple(event, $element) {
    const $ripple = $('<span class="ripple-effect"></span>');
    const offset = $element.offset();
    const x = event.pageX - offset.left;
    const y = event.pageY - offset.top;

    $ripple.css({
        left: x + 'px',
        top: y + 'px'
    });

    $element.css('position', 'relative').css('overflow', 'hidden');
    $element.append($ripple);

    setTimeout(() => $ripple.remove(), 600);
}

/**
 * Pulse animation for feedback
 */
function pulseAnimation($element) {
    $element.addClass('pulse');
    setTimeout(() => $element.removeClass('pulse'), 300);
}

/**
 * Apply preset configuration
 */
function applyPreset(preset) {
    switch (preset) {
        case 'solo':
            // 個人事業主優先
            $('#minScoreInput').val(20);
            $('#minScoreRange').val(20);
            $('#minScoreValue').text('20');
            $('#maxScoreInput').val('');
            $('#minWeakness').val(30);
            $('#minWeaknessValue').text('30');
            $('#solo-class-solo').prop('checked', true);
            $('#solo-class-small').prop('checked', true);
            $('#solo-class-unknown').prop('checked', false);
            $('#solo-class-corporate').prop('checked', false);
            break;

        case 'small':
            // 小規模事業者
            $('#minScoreInput').val(30);
            $('#minScoreRange').val(30);
            $('#minScoreValue').text('30');
            $('#maxScoreInput').val('');
            $('#minWeakness').val(0);
            $('#minWeaknessValue').text('0');
            $('#solo-class-solo').prop('checked', true);
            $('#solo-class-small').prop('checked', true);
            $('#solo-class-unknown').prop('checked', true);
            $('#solo-class-corporate').prop('checked', false);
            break;

        case 'all':
            // 全規模対象
            $('#minScoreInput').val(20);
            $('#minScoreRange').val(20);
            $('#minScoreValue').text('20');
            $('#maxScoreInput').val('');
            $('#minWeakness').val(0);
            $('#minWeaknessValue').text('0');
            $('#solo-class-solo').prop('checked', true);
            $('#solo-class-small').prop('checked', true);
            $('#solo-class-unknown').prop('checked', true);
            $('#solo-class-corporate').prop('checked', true);
            break;
    }
}

/**
 * Handle search form submission
 */
function handleSearch(e) {
    e.preventDefault();

    // Collect form data
    const region = $('#regionSelect').val();
    const prefecture = $('#prefectureSelect').val();
    const cities = $('input[name="city"]:checked').map(function() {
        return $(this).val();
    }).get();
    const businessTypes = $('input[name="business_type"]:checked').map(function() {
        return $(this).val();
    }).get();
    const limit = parseInt($('#limitSelect').val(), 10);

    let minScore = $('#minScoreInput').val();
    let maxScore = $('#maxScoreInput').val();
    minScore = minScore !== '' ? parseInt(minScore, 10) : null;
    maxScore = maxScore !== '' ? parseInt(maxScore, 10) : null;

    const soloClasses = $('input[name="solo_classification"]:checked').map(function() {
        return $(this).val();
    }).get();
    const soloScoreMin = $('#soloScoreMin').val();
    const soloScoreMax = $('#soloScoreMax').val();
    const minWeakness = parseInt($('#minWeakness').val(), 10) || 0;

    // AI verification settings
    const useAiVerify = $('#useAiVerify').is(':checked');
    const aiTopN = parseInt($('#aiTopN').val(), 10) || 30;

    // AI relevance gate settings
    const useAiRelevance = $('#useAiRelevance').is(':checked');
    const aiRelevanceTopN = parseInt($('#aiRelevanceTopN').val(), 10) || 30;

    // Validation
    if (!region) {
        showError('地方を選択してください');
        return;
    }

    if (!prefecture) {
        showError('都道府県を選択してください');
        return;
    }

    if (cities.length === 0) {
        showError('都市を少なくとも1つ選択してください');
        return;
    }

    if (businessTypes.length === 0) {
        showError('業種を少なくとも1つ選択してください');
        return;
    }

    // Save to localStorage
    saveLastSearch({
        region: region,
        prefecture: prefecture,
        cities: cities,
        businessTypes: businessTypes,
        limit: limit,
        minScore: minScore,
        maxScore: maxScore,
        soloClasses: soloClasses,
        soloScoreMin: soloScoreMin,
        soloScoreMax: soloScoreMax,
        minWeakness: minWeakness,
        useAiVerify: useAiVerify,
        aiTopN: aiTopN,
        useAiRelevance: useAiRelevance,
        aiRelevanceTopN: aiRelevanceTopN
    });

    // Update UI
    showLoading();
    hideError();
    hideResults();

    // Send search request
    $.ajax({
        url: '/api/search',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({
            prefecture: prefecture,
            cities: cities,
            business_types: businessTypes,
            limit: limit,
            min_score: minScore,
            max_score: maxScore,
            solo_classifications: soloClasses,
            solo_score_min: soloScoreMin,
            solo_score_max: soloScoreMax,
            min_weakness: minWeakness,
            use_ai_verify: useAiVerify,
            ai_top_n: aiTopN,
            use_ai_relevance: useAiRelevance,
            ai_relevance_top_n: aiRelevanceTopN
        }),
        success: function(response) {
            if (response.status === 'started') {
                startProgressPolling();
            } else {
                showError('検索の開始に失敗しました');
                resetSearchButton();
            }
        },
        error: function(xhr) {
            const message = xhr.responseJSON?.message || '検索リクエストに失敗しました';
            showError(message);
            resetSearchButton();
        }
    });
}

/**
 * Start polling for progress updates
 */
let progressPollInterval = null;

function startProgressPolling() {
    progressPollInterval = setInterval(function() {
        $.get('/api/progress', function(data) {
            updateProgress(data);

            if (data.status === 'completed') {
                clearInterval(progressPollInterval);
                progressPollInterval = null;
                allResults = data.results || [];
                csvPath = data.csv_path || '';
                displayResults(allResults, data.stats || {});
                resetSearchButton();
            } else if (data.status === 'error') {
                clearInterval(progressPollInterval);
                progressPollInterval = null;
                showError(data.message || '検索中にエラーが発生しました');
                resetSearchButton();
                hideProgress();
            } else if (data.status === 'cancelled') {
                clearInterval(progressPollInterval);
                progressPollInterval = null;
                showCancelledMessage();
                resetSearchButton();
                hideProgress();
            }
        }).fail(function() {
            clearInterval(progressPollInterval);
            progressPollInterval = null;
            showError('進捗状況の取得に失敗しました');
            resetSearchButton();
            hideProgress();
        });
    }, 1000);
}

/**
 * Cancel ongoing search
 */
function cancelSearch() {
    $.ajax({
        url: '/api/cancel',
        method: 'POST',
        contentType: 'application/json',
        success: function(response) {
            if (response.status === 'cancelled') {
                // Poll will detect the cancelled status
                console.log('Search cancelled');
            }
        },
        error: function(xhr) {
            console.warn('Failed to cancel search:', xhr.responseJSON?.message);
        }
    });
}

/**
 * Show cancelled message
 */
function showCancelledMessage() {
    const $section = $('#errorSection');
    const $message = $('#errorMessage');
    $message.text('検索が中止されました');
    $section.removeClass('alert-danger').addClass('alert-warning');
    $section.show();

    // Reset alert class after showing
    setTimeout(function() {
        $section.removeClass('alert-warning').addClass('alert-danger');
    }, 5000);
}

/**
 * Update progress display
 */
function updateProgress(data) {
    const $section = $('#progressSection');
    const $bar = $('#progressBar');
    const $percent = $('#progressPercent');
    const $message = $('#progressMessage');
    const $stats = $('#progressStats');
    const $title = $('#progressTitle');

    $section.show();

    if (data.total > 0) {
        const percent = Math.round((data.current / data.total) * 100);
        $bar.css('width', percent + '%');
        $percent.text(percent + '%');
    }

    $message.text(data.message || '処理中...');

    // Show stats if available
    if (data.stats) {
        const stats = data.stats;
        $stats.text(`収集: ${stats.total_collected || 0} | フィルター後: ${stats.precheck_kept || 0}`);
    }

    // Update title based on status
    switch (data.status) {
        case 'running':
            $title.text(`検索中... (${data.current}/${data.total})`);
            break;
        case 'completed':
            $title.text('検索完了');
            break;
        default:
            $title.text('検索中...');
    }
}

/**
 * Display search results with staggered animation
 */
function displayResults(results, stats) {
    hideProgress();
    stats = stats || {};

    const $section = $('#resultsSection');
    const $container = $('#resultsContainer');
    const $emptyState = $('#emptyState');

    // Fade in results section
    $section.hide().removeClass('d-none').fadeIn(300);
    $container.empty();

    // Remove any old summary lines
    $('#aiSummaryLine').remove();
    $('#funnelLine').remove();

    if (!results || results.length === 0) {
        $emptyState.show();
        updateResultsSummary(0, 0, 0, 0);
        return;
    }

    // Prioritize solo/small in display order.
    const classPriority = { solo: 3, small: 2, unknown: 1, corporate: 0 };
    const salesPriority = { '○': 2, '△': 1, '×': 0 };
    results = [...results].sort((a, b) => {
        const aClass = (a.solo_classification || 'unknown').toLowerCase();
        const bClass = (b.solo_classification || 'unknown').toLowerCase();
        const aSales = a.sales_label || '×';
        const bSales = b.sales_label || '×';
        const classDiff = (classPriority[bClass] || 0) - (classPriority[aClass] || 0);
        if (classDiff !== 0) return classDiff;
        const salesDiff = (salesPriority[bSales] || 0) - (salesPriority[aSales] || 0);
        if (salesDiff !== 0) return salesDiff;
        return (b.lead_score || b.score || 0) - (a.lead_score || a.score || 0);
    });

    $emptyState.hide();

    // AI summary line above results
    let aiSummaryParts = [];
    const aiFilterStats = stats.ai_filter_stats;
    if (aiFilterStats && !aiFilterStats.error) {
        const byFlag = aiFilterStats.by_flag || {};
        const flagDetails = Object.entries(byFlag).map(([k, v]) => `${k}: ${v}`).join(', ');
        aiSummaryParts.push(
            `AIフィルタ: ${aiFilterStats.checked || 0}件検査 / ${aiFilterStats.kept || 0}件保持 / ${aiFilterStats.dropped || 0}件除外` +
            (flagDetails ? ` (${flagDetails})` : '')
        );
    }
    const aiStats = stats.ai_stats;
    if (aiStats && !aiStats.error) {
        aiSummaryParts.push(
            `AI弱さ検証: ${aiStats.checked || 0}件検査 / 弱${aiStats.confirmed_weak || 0}件 / 強${aiStats.confirmed_strong || 0}件`
        );
    }
    if (aiSummaryParts.length > 0) {
        const summaryHtml = `
            <div id="aiSummaryLine" class="alert alert-info py-2 mb-3" style="font-size: 0.85rem;">
                <i class="bi bi-robot"></i> ${aiSummaryParts.join(' | ')}
            </div>
        `;
        $container.before(summaryHtml);
    }

    // Pipeline funnel stats
    const funnel = stats.funnel;
    if (funnel) {
        const overseasBlocked = funnel.jp_overseas_blocked || 0;
        const jpFilterNote = overseasBlocked > 0 ? ` <span class="text-danger">(海外${overseasBlocked}件除外)</span>` : '';
        const funnelHtml = `
            <div id="funnelLine" class="alert alert-light py-2 mb-3" style="font-size: 0.8rem; border-left: 3px solid var(--bs-secondary);">
                <i class="bi bi-funnel"></i>
                <strong>収集プロセス:</strong>
                クエリ ${funnel.queries_run || 0}本
                → URL ${funnel.urls_collected || 0}件
                → JP ${funnel.urls_after_jp_filter || '?'}件${jpFilterNote}
                → フィルタ後 ${funnel.urls_after_hardfilter || 0}件
                → チェック後 ${funnel.urls_after_precheck || 0}件
                → 処理 ${funnel.urls_processed || 0}件
                → <strong>最終 ${funnel.leads_final || 0}件</strong>
            </div>
        `;
        $container.before(funnelHtml);
    }

    // Calculate statistics
    let soloCount = 0;
    let smallCount = 0;
    let weakCount = 0;
    let totalWeakness = 0;

    results.forEach(function(lead, index) {
        const classification = (lead.solo_classification || 'unknown').toLowerCase();

        if (classification === 'solo') soloCount++;
        if (classification === 'small') smallCount++;
        if (lead.weakness_score >= 50) weakCount++;
        totalWeakness += lead.weakness_score || 0;

        const card = createResultCard(lead, index);
        $container.append(card);
    });

    const avgWeakness = results.length > 0 ? Math.round(totalWeakness / results.length) : 0;

    // Update summary with counting animation
    animateCounter('#totalLeadsCount', results.length);
    animateCounter('#soloCount', soloCount);
    animateCounter('#smallCount', smallCount);
    animateCounter('#avgWeakness', avgWeakness);

    // Update filter counts
    $('#filterAllCount').text(results.length);
    $('#filterSoloCount').text(soloCount);
    $('#filterSmallCount').text(smallCount);
    $('#filterWeakCount').text(weakCount);

    // Staggered card animation
    animateCards();

    // Scroll to results smoothly
    setTimeout(function() {
        $('html, body').animate({
            scrollTop: $section.offset().top - 80
        }, 600, 'swing');
    }, 200);
}

/**
 * Animate cards with stagger effect
 */
function animateCards() {
    const $cards = $('.result-card');
    $cards.each(function(index) {
        const $card = $(this);
        $card.css({
            'opacity': '0',
            'transform': 'translateY(20px)'
        });

        setTimeout(function() {
            $card.css({
                'transition': 'opacity 0.4s ease, transform 0.4s ease',
                'opacity': '1',
                'transform': 'translateY(0)'
            });
        }, Math.min(index * 50, 500)); // Cap at 500ms total delay
    });
}

/**
 * Animate number counting up
 */
function animateCounter(selector, target) {
    const $el = $(selector);
    const duration = 600;
    const start = 0;
    const startTime = performance.now();

    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);

        // Ease out cubic
        const eased = 1 - Math.pow(1 - progress, 3);
        const current = Math.round(start + (target - start) * eased);

        $el.text(current);

        if (progress < 1) {
            requestAnimationFrame(update);
        }
    }

    requestAnimationFrame(update);
}

/**
 * Update results summary cards (for non-animated updates)
 */
function updateResultsSummary(total, solo, small, avgWeakness) {
    // Use simple text update for initial/empty states
    $('#totalLeadsCount').text(total);
    $('#soloCount').text(solo);
    $('#smallCount').text(small);
    $('#avgWeakness').text(avgWeakness);
}

/**
 * Create a result card element
 */
function createResultCard(lead, index) {
    let displayName = lead.name || '名称不明';

    const grade = (lead.grade || 'C').toUpperCase();
    const gradeClass = `grade-${grade.toLowerCase()}`;
    const classification = (lead.solo_classification || 'unknown').toLowerCase();

    // Classification badge
    const classificationLabels = {
        'solo': { text: '個人事業主', class: 'badge-solo' },
        'small': { text: '小規模', class: 'badge-small' },
        'corporate': { text: '法人', class: 'badge-corporate' },
        'unknown': { text: '不明', class: 'badge-unknown' }
    };
    const classInfo = classificationLabels[classification] || classificationLabels['unknown'];

    // Score pill class
    const getScoreClass = (score) => {
        if (score >= 60) return 'score-high';
        if (score >= 40) return 'score-medium';
        return 'score-low';
    };

    // Sales label badge
    const salesLabel = lead.sales_label || '×';
    const salesLabelClass = salesLabel === '○' ? 'bg-success' : salesLabel === '△' ? 'bg-warning text-dark' : 'bg-secondary';
    const salesLabelHtml = `
        <span class="badge ${salesLabelClass}" style="font-size: 1rem; margin-right: 4px;" title="${escapeHtml(lead.sales_reason || '')}">
            ${escapeHtml(salesLabel)}
        </span>
    `;

    // Weakness reasons
    let weaknessHtml = '';
    if (lead.weakness_reasons && lead.weakness_reasons.length > 0) {
        const reasons = lead.weakness_reasons.slice(0, 2);
        weaknessHtml = `
            <div class="weakness-indicators">
                ${reasons.map(r => `<span class="weakness-tag">${escapeHtml(r)}</span>`).join('')}
            </div>
        `;
    }

    // AI verification badge (weakness verify)
    let aiHtml = '';
    if (lead.ai_verified === true) {
        aiHtml += `
            <div class="ai-verification mt-2">
                <span class="badge bg-warning text-dark">
                    <i class="bi bi-robot"></i> AI弱さ判定: WEAK
                    ${lead.ai_confidence ? `(確信度: ${lead.ai_confidence}/10)` : ''}
                </span>
                ${lead.ai_reason ? `<small class="text-muted d-block">${escapeHtml(lead.ai_reason)}</small>` : ''}
            </div>
        `;
    } else if (lead.ai_verified === false && lead.ai_confidence > 0) {
        aiHtml += `
            <div class="ai-verification mt-2">
                <span class="badge bg-success">
                    <i class="bi bi-robot"></i> AI弱さ判定: NOT_WEAK
                    ${lead.ai_confidence ? `(確信度: ${lead.ai_confidence}/10)` : ''}
                </span>
                ${lead.ai_reason ? `<small class="text-muted d-block">${escapeHtml(lead.ai_reason)}</small>` : ''}
            </div>
        `;
    }

    // AI filter badge (post-crawl relevance)
    if (lead.ai_action === 'KEEP' && lead.ai_filter_confidence > 0) {
        aiHtml += `
            <div class="ai-filter mt-1">
                <span class="badge bg-info">
                    <i class="bi bi-funnel"></i> AIフィルタ: KEEP
                    (確信度: ${lead.ai_filter_confidence}/10)
                </span>
            </div>
        `;
    } else if (lead.ai_action === 'DROP') {
        const flagsText = (lead.ai_flags || []).join(', ');
        aiHtml += `
            <div class="ai-filter mt-1">
                <span class="badge bg-danger">
                    <i class="bi bi-funnel"></i> AIフィルタ: DROP
                    (確信度: ${lead.ai_filter_confidence}/10)
                </span>
                ${flagsText ? `<small class="text-danger d-block">${escapeHtml(flagsText)}</small>` : ''}
                ${lead.ai_filter_reason ? `<small class="text-muted d-block">${escapeHtml(lead.ai_filter_reason)}</small>` : ''}
            </div>
        `;
    }

    // Truncate URL for display
    const displayUrl = truncateUrl(lead.url, 50);

    const leadScore = lead.lead_score || lead.score || 0;
    const soloScore100 = lead.solo_score_100 || 0;

    const html = `
        <div class="result-card ${gradeClass}" data-index="${index}"
             data-classification="${classification}"
             data-weakness="${lead.weakness_score || 0}"
             data-sales-label="${escapeHtml(salesLabel)}">
            <div class="result-card-header">
                <h6 class="result-card-title text-truncate-2">
                    ${salesLabelHtml}
                    ${escapeHtml(displayName)}
                </h6>
                <div class="result-card-badges">
                    <span class="badge ${classInfo.class}">${classInfo.text}</span>
                </div>
            </div>

            <a href="${escapeHtml(lead.url)}" target="_blank" rel="noopener" class="result-card-url">
                <i class="bi bi-link-45deg"></i> ${escapeHtml(displayUrl)}
            </a>

            <div class="result-card-meta">
                ${lead.business_type ? `<span><i class="bi bi-briefcase"></i> ${escapeHtml(lead.business_type)}</span>` : ''}
                ${lead.city ? `<span><i class="bi bi-geo-alt"></i> ${escapeHtml(lead.city)}</span>` : ''}
                ${lead.phone ? `<span><i class="bi bi-telephone"></i> ${escapeHtml(lead.phone)}</span>` : ''}
            </div>

            <div class="result-card-scores">
                <span class="score-pill ${getScoreClass(leadScore)}">
                    <i class="bi bi-speedometer2"></i> Lead Score ${leadScore}
                </span>
                <span class="score-pill ${lead.weakness_score >= 50 ? 'score-high' : 'score-low'}">
                    <i class="bi bi-graph-down"></i> 弱さ ${lead.weakness_score || 0}
                </span>
                <span class="score-pill">
                    <i class="bi bi-person"></i> 個人度 ${soloScore100}
                </span>
            </div>

            ${weaknessHtml}
            ${aiHtml}
        </div>
    `;

    return html;
}

/**
 * Handle CSV download
 */
function handleDownload() {
    if (!csvPath) {
        showError('ダウンロードファイルが見つかりません');
        return;
    }

    window.location.href = `/api/download/${csvPath}`;
}

/**
 * Show loading state
 */
function showLoading() {
    const $btn = $('#searchBtn');
    $btn.prop('disabled', true);
    $btn.addClass('btn-loading');
    $('#progressSection').show();
}

/**
 * Reset search button
 */
function resetSearchButton() {
    const $btn = $('#searchBtn');
    $btn.prop('disabled', false);
    $btn.removeClass('btn-loading');
}

/**
 * Show error message
 */
function showError(message) {
    const $section = $('#errorSection');
    const $message = $('#errorMessage');
    $message.text(message);
    $section.show();

    // Scroll to error
    $('html, body').animate({
        scrollTop: $section.offset().top - 100
    }, 300);
}

/**
 * Hide error message
 */
function hideError() {
    $('#errorSection').hide();
}

/**
 * Hide progress section
 */
function hideProgress() {
    $('#progressSection').hide();
}

/**
 * Hide results section
 */
function hideResults() {
    $('#resultsSection').hide();
}

/**
 * Save search parameters to localStorage
 */
function saveLastSearch(data) {
    try {
        localStorage.setItem('leadFinder_lastSearch', JSON.stringify(data));
    } catch (e) {
        console.warn('Failed to save search to localStorage:', e);
    }
}

/**
 * Load last search from localStorage
 */
function loadLastSearch() {
    try {
        const saved = localStorage.getItem('leadFinder_lastSearch');
        if (!saved) return;

        const data = JSON.parse(saved);

        // Restore region
        if (data.region) {
            $('#regionSelect').val(data.region).trigger('change');

            // Wait for prefectures to load, then restore prefecture
            setTimeout(function() {
                if (data.prefecture) {
                    $('#prefectureSelect').val(data.prefecture).trigger('change');

                    // Wait for cities to load, then restore cities
                    setTimeout(function() {
                        if (data.cities && data.cities.length > 0) {
                            data.cities.forEach(function(city) {
                                $(`input[name="city"][value="${city}"]`).prop('checked', true);
                            });
                            updateSelectedCityCount();
                        }
                    }, 500);
                }
            }, 300);
        }

        // Restore business types
        if (data.businessTypes && data.businessTypes.length > 0) {
            data.businessTypes.forEach(function(btype) {
                $(`input[name="business_type"][value="${btype}"]`).prop('checked', true);
            });
        }

        // Restore other settings
        if (data.limit) {
            $('#limitSelect').val(data.limit);
        }

        if (data.minScore !== null && data.minScore !== undefined) {
            $('#minScoreInput').val(data.minScore);
            $('#minScoreRange').val(data.minScore);
            $('#minScoreValue').text(data.minScore);
        }

        if (data.maxScore !== null && data.maxScore !== undefined) {
            $('#maxScoreInput').val(data.maxScore);
        }

        // Restore solo classifications
        if (data.soloClasses && data.soloClasses.length > 0) {
            $('input[name="solo_classification"]').prop('checked', false);
            data.soloClasses.forEach(function(cls) {
                $(`input[name="solo_classification"][value="${cls}"]`).prop('checked', true);
            });
        }

        // Restore solo score range
        if (data.soloScoreMin !== undefined && data.soloScoreMin !== null && data.soloScoreMin !== '') {
            $('#soloScoreMin').val(data.soloScoreMin);
        }
        if (data.soloScoreMax !== undefined && data.soloScoreMax !== null && data.soloScoreMax !== '') {
            $('#soloScoreMax').val(data.soloScoreMax);
        }

        // Restore weakness filter
        if (data.minWeakness !== undefined && data.minWeakness !== null) {
            $('#minWeakness').val(data.minWeakness);
            $('#minWeaknessValue').text(data.minWeakness);
        }

        // Restore AI verification settings
        if (data.useAiVerify !== undefined) {
            $('#useAiVerify').prop('checked', data.useAiVerify);
            if (data.useAiVerify) {
                $('#aiTopNContainer').show();
            }
        }
        if (data.aiTopN !== undefined && data.aiTopN !== null) {
            $('#aiTopN').val(data.aiTopN);
        }

    } catch (e) {
        console.warn('Failed to load search from localStorage:', e);
    }
}

/**
 * Utility: Escape HTML
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Utility: Truncate URL
 */
function truncateUrl(url, maxLength = 50) {
    if (!url) return '';
    if (url.length <= maxLength) return url;
    return url.substring(0, maxLength) + '...';
}

/**
 * Update selected city count (exposed globally for HTML use)
 */
function updateSelectedCityCount() {
    const count = $('input[name="city"]:checked').length;
    $('#selectedCityCount').text(`${count}件選択中`);
}

/**
 * Header scroll shadow effect
 */
(function() {
    const header = document.querySelector('.app-header');
    if (!header) return;

    window.addEventListener('scroll', function() {
        if (window.pageYOffset > 100) {
            header.style.boxShadow = '0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1)';
        } else {
            header.style.boxShadow = '0 1px 2px 0 rgb(0 0 0 / 0.05)';
        }
    });
})();
