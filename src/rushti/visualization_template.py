"""Embedded HTML template for DAG visualization.

This template is embedded as a Python string constant so that it works
in both normal Python and PyInstaller-frozen executables without needing
external file dependencies. The dashboard (dashboard.py) uses the same
approach with its HTML embedded as an f-string.

Uses string.Template substitution ($variable) for:
  - $title, $logo_svg, $dashboard_link_html, $legend_html
  - $nodes_json, $edges_json, $stage_colors_json
"""

VISUALIZATION_TEMPLATE = r"""<!DOCTYPE html>
<html>
<head>
    <title>$title</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style type="text/css">
        :root {
            --bg-primary: #F8FAFC;
            --bg-secondary: #FFFFFF;
            --bg-tertiary: #F1F5F9;
            --accent-primary: #00AEEF;
            --accent-secondary: #FBB040;
            --text-primary: #1E293B;
            --text-secondary: #64748B;
            --text-muted: #94A3B8;
            --border-color: #E2E8F0;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background-color: var(--bg-primary);
            color: var(--text-primary);
            overflow: hidden;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }

        #header {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            padding: 16px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            height: 64px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        }

        #header h1 {
            font-size: 1.25em;
            font-weight: 700;
            letter-spacing: -0.5px;
            color: var(--text-primary);
        }

        #header .subtitle {
            font-size: 0.8em;
            color: var(--text-secondary);
        }

        #header a {
            color: var(--accent-primary);
            text-decoration: none;
        }

        #toolbar {
            background: var(--bg-secondary);
            padding: 12px 24px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            min-height: 60px;
        }

        #toolbar .left {
            display: flex;
            align-items: center;
            gap: 20px;
            flex-wrap: wrap;
        }

        #toolbar .right {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .legend-item {
            display: inline-flex;
            align-items: center;
            margin-right: 12px;
            font-size: 0.875em;
            cursor: pointer;
            padding: 4px 10px;
            border-radius: 8px;
            transition: all 0.3s ease;
            color: var(--text-secondary);
        }

        .legend-item:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }

        .legend-item.filtered {
            opacity: 0.3;
        }

        .legend-color {
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid currentColor;
            margin-right: 6px;
            border-radius: 4px;
        }

        #searchBox {
            padding: 10px 16px;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            width: 280px;
            font-size: 0.9em;
            background: var(--bg-tertiary);
            color: var(--text-primary);
            transition: all 0.3s ease;
        }

        #searchBox::placeholder {
            color: var(--text-muted);
        }

        #searchBox:focus {
            outline: none;
            border-color: var(--accent-primary);
            box-shadow: 0 0 0 3px rgba(0, 174, 239, 0.15);
            background: var(--bg-secondary);
        }

        .view-btn {
            padding: 10px 18px;
            border: 1px solid var(--border-color);
            background: var(--bg-secondary);
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 0.875em;
            font-weight: 500;
            transition: all 0.3s ease;
        }

        .view-btn:first-child {
            border-radius: 8px 0 0 8px;
        }

        .view-btn:last-child {
            border-radius: 0 8px 8px 0;
        }

        .view-btn:not(:last-child) {
            border-right: none;
        }

        .view-btn:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }

        .view-btn.active {
            background: var(--accent-primary);
            color: white;
            border-color: transparent;
        }

        #main-container {
            display: flex;
            height: calc(100vh - 124px);
        }

        #content-area {
            flex: 1;
            position: relative;
            overflow: hidden;
        }

        #mynetwork {
            width: 100%;
            height: 100%;
            background-color: var(--bg-primary);
        }

        #tableView {
            width: 100%;
            height: 100%;
            overflow: auto;
            display: none;
            background: var(--bg-primary);
        }

        #taskTable {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }

        #taskTable th {
            background: var(--bg-primary);
            padding: 14px 16px;
            text-align: left;
            border-bottom: 2px solid var(--border-color);
            position: sticky;
            top: 0;
            cursor: pointer;
            white-space: nowrap;
            color: var(--text-primary);
            font-weight: 600;
            transition: all 0.3s ease;
            z-index: 10;
        }

        #taskTable th:hover {
            background: var(--bg-tertiary);
        }

        #taskTable th .sort-icon {
            margin-left: 5px;
            opacity: 0.3;
        }

        #taskTable th.sorted .sort-icon {
            opacity: 1;
            color: var(--accent-primary);
        }

        #taskTable td {
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-color);
            vertical-align: top;
            color: var(--text-secondary);
        }

        #taskTable tr:hover {
            background: var(--bg-tertiary);
        }

        #taskTable tr.selected {
            background: rgba(0, 174, 239, 0.08);
        }

        #taskTable .stage-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.85em;
            font-weight: 500;
            color: white;
        }

        #taskTable .params-cell {
            max-width: 300px;
            font-family: 'SF Mono', 'Fira Code', monospace;
            font-size: 0.85em;
            white-space: pre-wrap;
            word-break: break-all;
        }

        #sidebar {
            width: 360px;
            background: var(--bg-secondary);
            border-left: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
            transition: width 0.3s ease;
        }

        #sidebar.collapsed {
            width: 0;
            overflow: hidden;
        }

        #sidebar-header {
            padding: 20px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        #sidebar-header h3 {
            font-size: 1.1em;
            font-weight: 600;
            color: var(--text-primary);
        }

        #sidebar-toggle {
            background: none;
            border: none;
            cursor: pointer;
            font-size: 1.3em;
            color: var(--text-muted);
            transition: all 0.3s ease;
            width: 32px;
            height: 32px;
            border-radius: 6px;
        }

        #sidebar-toggle:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }

        #sidebar-content {
            flex: 1;
            overflow-y: auto;
            padding: 20px;
        }

        #sidebar-content::-webkit-scrollbar {
            width: 8px;
        }

        #sidebar-content::-webkit-scrollbar-track {
            background: var(--bg-tertiary);
        }

        #sidebar-content::-webkit-scrollbar-thumb {
            background: #CBD5E1;
            border-radius: 4px;
        }

        #sidebar-content::-webkit-scrollbar-thumb:hover {
            background: var(--text-muted);
        }

        .detail-section {
            margin-bottom: 24px;
        }

        .detail-section h4 {
            font-size: 0.75em;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 10px;
            font-weight: 600;
        }

        .detail-value {
            font-size: 0.95em;
            padding: 10px 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            word-break: break-word;
            color: var(--text-primary);
        }

        .detail-value.stage {
            display: inline-block;
            font-weight: 500;
            color: white;
            border: none;
        }

        .detail-list {
            list-style: none;
        }

        .detail-list li {
            padding: 8px 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            margin-bottom: 6px;
            border-radius: 8px;
            font-size: 0.9em;
            color: var(--text-secondary);
            transition: all 0.3s ease;
        }

        .detail-list li:hover {
            background: var(--bg-tertiary);
            border-color: #CBD5E1;
        }

        .params-table {
            width: 100%;
            font-size: 0.85em;
        }

        .params-table td {
            padding: 8px 10px;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-secondary);
        }

        .params-table td:first-child {
            font-weight: 600;
            white-space: nowrap;
            width: 40%;
            color: var(--text-primary);
        }

        .params-table td:last-child {
            font-family: 'SF Mono', 'Fira Code', monospace;
            word-break: break-all;
        }

        #placeholder-message {
            color: var(--text-muted);
            text-align: center;
            padding: 60px 20px;
            font-size: 0.95em;
        }

        #stats-bar {
            position: absolute;
            bottom: 20px;
            left: 20px;
            background: var(--bg-secondary);
            padding: 10px 18px;
            border-radius: 12px;
            border: 1px solid var(--border-color);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
            font-size: 0.875em;
            z-index: 100;
            color: var(--text-secondary);
        }

        #stats-bar strong {
            color: var(--text-primary);
        }

        #dag-controls {
            position: absolute;
            bottom: 20px;
            right: 20px;
            background: var(--bg-secondary);
            padding: 10px;
            border-radius: 12px;
            border: 1px solid var(--border-color);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
            z-index: 100;
            display: flex;
            gap: 8px;
        }

        .ctrl-btn {
            padding: 8px 14px;
            border: 1px solid var(--border-color);
            background: var(--bg-secondary);
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 0.85em;
            font-weight: 500;
            border-radius: 8px;
            transition: all 0.3s ease;
        }

        .ctrl-btn:hover {
            background: var(--bg-tertiary);
            border-color: #CBD5E1;
            color: var(--text-primary);
            transform: translateY(-2px);
        }

        .ctrl-btn.active {
            background: var(--accent-primary);
            color: white;
            border-color: transparent;
        }

        .hidden {
            display: none !important;
        }

        .match-count {
            font-size: 0.875em;
            color: var(--text-muted);
            margin-left: 10px;
        }

        .sidebar-toggle-btn {
            padding: 10px 18px;
            border: 1px solid transparent;
            background: var(--accent-primary);
            color: white;
            cursor: pointer;
            font-size: 0.875em;
            font-weight: 500;
            border-radius: 8px;
            transition: all 0.3s ease;
        }

        .sidebar-toggle-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 16px rgba(0, 174, 239, 0.3);
        }
    </style>
</head>
<body>
    <div id="header">
        <div style="display:flex;align-items:center;gap:16px;">
            $logo_svg
            <div>
                <h1>DAG Visualization</h1>
                <div class="subtitle">Interactive Task Dependency Graph</div>
            </div>
        </div>
        <div style="display:flex;align-items:center;gap:16px;">
            $dashboard_link_html
        </div>
    </div>
    <div id="toolbar">
        <div class="left">
            <div class="view-buttons">
                <button class="view-btn active" data-view="compact">Compact</button>
                <button class="view-btn" data-view="detailed">Detailed</button>
                <button class="view-btn" data-view="table">Table</button>
            </div>
            <div class="legend">
                $legend_html
            </div>
        </div>
        <div class="right">
            <input type="text" id="searchBox" placeholder="Search ID, process, instance, stage, parameters..." />
            <span id="matchCount" class="match-count"></span>
            <button id="btnToggleSidebar" class="sidebar-toggle-btn" onclick="toggleSidebar()" title="Toggle Task Details panel">&gt;&gt; Details</button>
        </div>
    </div>
    <div id="main-container">
        <div id="content-area">
            <div id="mynetwork"></div>
            <div id="tableView">
                <table id="taskTable">
                    <thead>
                        <tr>
                            <th data-sort="id">ID <span class="sort-icon">&#8597;</span></th>
                            <th data-sort="process">Process <span class="sort-icon">&#8597;</span></th>
                            <th data-sort="instance">Instance <span class="sort-icon">&#8597;</span></th>
                            <th data-sort="stage">Stage <span class="sort-icon">&#8597;</span></th>
                            <th data-sort="predecessors">Predecessors <span class="sort-icon">&#8597;</span></th>
                            <th>Parameters</th>
                            <th>Options</th>
                        </tr>
                    </thead>
                    <tbody id="tableBody"></tbody>
                </table>
            </div>
            <div id="stats-bar">
                <strong>Tasks:</strong> <span id="taskCount">{len(nodes)}</span> &nbsp;|&nbsp;
                <strong>Dependencies:</strong> <span id="depCount">{len(edges)}</span>
                <span id="filterStatus"></span>
            </div>
            <div id="dag-controls">
                <button class="ctrl-btn" onclick="network.fit()" title="Fit to screen">Fit</button>
                <button class="ctrl-btn" onclick="zoomIn()" title="Zoom in">+</button>
                <button class="ctrl-btn" onclick="zoomOut()" title="Zoom out">−</button>
                <button class="ctrl-btn" id="btnPhysics" onclick="togglePhysics()" title="Toggle physics">Physics</button>
            </div>
        </div>
        <div id="sidebar">
            <div id="sidebar-header">
                <h3>Task Details</h3>
                <button id="sidebar-toggle" onclick="toggleSidebar()">×</button>
            </div>
            <div id="sidebar-content">
                <div id="placeholder-message">
                    Click on a task to view details
                </div>
                <div id="task-details" class="hidden">
                    <div class="detail-section">
                        <h4>Task ID</h4>
                        <div class="detail-value" id="detail-id"></div>
                    </div>
                    <div class="detail-section">
                        <h4>Process</h4>
                        <div class="detail-value" id="detail-process"></div>
                    </div>
                    <div class="detail-section">
                        <h4>Instance</h4>
                        <div class="detail-value" id="detail-instance"></div>
                    </div>
                    <div class="detail-section">
                        <h4>Stage</h4>
                        <div class="detail-value stage" id="detail-stage"></div>
                    </div>
                    <div class="detail-section">
                        <h4>Predecessors</h4>
                        <ul class="detail-list" id="detail-predecessors"></ul>
                        <div class="detail-value" id="detail-no-predecessors" style="display:none;">None</div>
                    </div>
                    <div class="detail-section" id="params-section">
                        <h4>Parameters</h4>
                        <table class="params-table" id="detail-params"></table>
                    </div>
                    <div class="detail-section" id="options-section">
                        <h4>Execution Options</h4>
                        <table class="params-table" id="detail-options"></table>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script type="text/javascript">
        // Data
        var nodesData = $nodes_json;
        var edgesData = $edges_json;
        var stageColors = $stage_colors_json;

        // State
        var currentView = 'compact';
        var physicsEnabled = false;
        var selectedTaskId = null;
        var filteredStages = new Set();
        var searchTerm = '';
        var sortColumn = 'id';
        var sortDirection = 'asc';

        // Create vis.js datasets
        var nodes = new vis.DataSet(nodesData);
        var edges = new vis.DataSet(edgesData);

        // Network setup
        var container = document.getElementById("mynetwork");
        var data = { nodes: nodes, edges: edges };

        var options = {
            nodes: {
                shape: "box",
                margin: 12,
                font: {
                    size: 14,
                    face: "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                    color: "#ffffff",
                    multi: "md",
                    bold: { color: "#ffffff" }
                },
                shadow: {
                    enabled: true,
                    color: "rgba(0, 0, 0, 0.1)",
                    size: 8,
                    x: 0,
                    y: 2
                },
                borderWidth: 2,
                borderWidthSelected: 3,
                widthConstraint: { minimum: 60 },
                shapeProperties: {
                    borderRadius: 8
                }
            },
            edges: {
                arrows: { to: { enabled: true, scaleFactor: 0.9 } },
                color: {
                    color: "rgba(100, 116, 139, 0.4)",
                    highlight: "#00AEEF",
                    hover: "#00AEEF"
                },
                smooth: {
                    type: "cubicBezier",
                    forceDirection: "horizontal",
                    roundness: 0.5
                },
                width: 2,
                selectionWidth: 3
            },
            layout: {
                hierarchical: {
                    enabled: true,
                    direction: "LR",
                    sortMethod: "directed",
                    levelSeparation: 200,
                    nodeSpacing: 120,
                    treeSpacing: 200
                }
            },
            physics: { enabled: false },
            interaction: {
                hover: true,
                tooltipDelay: 150,
                navigationButtons: false,
                keyboard: true,
                zoomView: true,
                zoomSpeed: 0.8
            }
        };

        var network = new vis.Network(container, data, options);

        // View mode switching
        document.querySelectorAll('.view-btn').forEach(function(btn) {
            btn.addEventListener('click', function() {
                var view = this.dataset.view;
                setView(view);
            });
        });

        function setView(view) {
            currentView = view;

            // Update buttons
            document.querySelectorAll('.view-btn').forEach(function(btn) {
                btn.classList.toggle('active', btn.dataset.view === view);
            });

            // Show/hide views
            var networkEl = document.getElementById('mynetwork');
            var tableEl = document.getElementById('tableView');
            var dagControls = document.getElementById('dag-controls');

            if (view === 'table') {
                networkEl.classList.add('hidden');
                tableEl.style.display = 'block';
                dagControls.classList.add('hidden');
                renderTable();
            } else {
                networkEl.classList.remove('hidden');
                tableEl.style.display = 'none';
                dagControls.classList.remove('hidden');

                // Update node labels
                var updates = [];
                nodesData.forEach(function(node) {
                    if (isNodeVisible(node)) {
                        updates.push({
                            id: node.id,
                            label: view === 'compact' ? node.compactLabel : node.detailedLabel,
                            hidden: false
                        });
                    }
                });
                nodes.update(updates);

                // Adjust layout - detailed view needs more spacing for larger nodes
                var levelSep = view === 'detailed' ? 350 : 200;
                var nodeSep = view === 'detailed' ? 200 : 100;
                network.setOptions({
                    layout: { hierarchical: { levelSeparation: levelSep, nodeSpacing: nodeSep } }
                });

                setTimeout(function() { network.fit(); }, 100);
            }
        }

        // Search functionality
        var searchBox = document.getElementById('searchBox');
        searchBox.addEventListener('input', function() {
            searchTerm = this.value.toLowerCase();
            applyFilters();
        });

        // Stage filtering (click on legend)
        document.querySelectorAll('.legend-item').forEach(function(item) {
            item.addEventListener('click', function() {
                var stage = this.dataset.stage;
                if (filteredStages.has(stage)) {
                    filteredStages.delete(stage);
                    this.classList.remove('filtered');
                } else {
                    filteredStages.add(stage);
                    this.classList.add('filtered');
                }
                applyFilters();
            });
        });

        function isNodeVisible(node) {
            // Check stage filter
            if (filteredStages.size > 0 && filteredStages.has(node.stage)) {
                return false;
            }
            // Check search (includes parameters)
            if (searchTerm) {
                var searchIn = (node.id + ' ' + node.process + ' ' + node.instance + ' ' + node.stage).toLowerCase();
                // Also search in parameter names and values
                if (node.parameters) {
                    Object.entries(node.parameters).forEach(function(p) {
                        searchIn += ' ' + p[0].toLowerCase() + ' ' + String(p[1]).toLowerCase();
                    });
                }
                if (!searchIn.includes(searchTerm)) {
                    return false;
                }
            }
            return true;
        }

        function applyFilters() {
            var visibleCount = 0;
            var updates = [];

            nodesData.forEach(function(node) {
                var visible = isNodeVisible(node);
                if (visible) visibleCount++;
                updates.push({
                    id: node.id,
                    hidden: !visible
                });
            });

            nodes.update(updates);

            // Update match count
            var matchEl = document.getElementById('matchCount');
            var filterStatus = document.getElementById('filterStatus');
            if (searchTerm || filteredStages.size > 0) {
                matchEl.textContent = visibleCount + ' of ' + nodesData.length + ' shown';
                filterStatus.textContent = ' (filtered)';
            } else {
                matchEl.textContent = '';
                filterStatus.textContent = '';
            }

            // Update table if visible
            if (currentView === 'table') {
                renderTable();
            }

            setTimeout(function() { network.fit(); }, 100);
        }

        // Table rendering
        function renderTable() {
            var tbody = document.getElementById('tableBody');
            var rows = [];

            // Filter and sort data
            var filteredData = nodesData.filter(isNodeVisible);

            filteredData.sort(function(a, b) {
                var valA, valB;
                if (sortColumn === 'predecessors') {
                    valA = a.predecessors.length;
                    valB = b.predecessors.length;
                } else if (sortColumn === 'id') {
                    // Smart numeric/string sorting for IDs
                    var strA = (a.id || '').toString();
                    var strB = (b.id || '').toString();
                    var numA = parseFloat(strA);
                    var numB = parseFloat(strB);
                    // If both are purely numeric, sort numerically
                    if (!isNaN(numA) && !isNaN(numB) && strA === numA.toString() && strB === numB.toString()) {
                        valA = numA;
                        valB = numB;
                    } else {
                        // Natural sort for mixed alphanumeric (e.g., "task1", "task2", "task10")
                        return sortDirection === 'asc'
                            ? strA.localeCompare(strB, undefined, {numeric: true, sensitivity: 'base'})
                            : strB.localeCompare(strA, undefined, {numeric: true, sensitivity: 'base'});
                    }
                } else {
                    valA = (a[sortColumn] || '').toString().toLowerCase();
                    valB = (b[sortColumn] || '').toString().toLowerCase();
                }
                if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
                if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
                return 0;
            });

            filteredData.forEach(function(node) {
                var stageColor = stageColors[node.stage] || stageColors['default'];
                var paramsHtml = '';
                if (node.parameters && Object.keys(node.parameters).length > 0) {
                    paramsHtml = Object.entries(node.parameters)
                        .map(function(p) { return p[0] + '=' + p[1]; })
                        .join('\\n');
                }
                var predsHtml = node.predecessors.length > 0 ? node.predecessors.join(', ') : '-';

                // Build options summary
                var optionsList = [];
                if (node.timeout !== null && node.timeout !== undefined) {
                    optionsList.push('timeout=' + node.timeout + 's');
                }
                if (node.cancel_at_timeout) {
                    optionsList.push('cancel_at_timeout');
                }
                if (node.safe_retry) {
                    optionsList.push('safe_retry');
                }
                if (!node.require_predecessor_success) {
                    optionsList.push('no_require_pred_success');
                }
                if (node.succeed_on_minor_errors) {
                    optionsList.push('succeed_on_minor_errors');
                }
                var optionsHtml = optionsList.length > 0 ? optionsList.join('\\n') : '-';

                rows.push(
                    '<tr data-id="' + node.id + '" class="' + (node.id === selectedTaskId ? 'selected' : '') + '">' +
                    '<td>' + node.id + '</td>' +
                    '<td>' + node.process + '</td>' +
                    '<td>' + (node.instance || '-') + '</td>' +
                    '<td><span class="stage-badge" style="background-color:' + stageColor + '">' + node.stage + '</span></td>' +
                    '<td>' + predsHtml + '</td>' +
                    '<td class="params-cell">' + paramsHtml + '</td>' +
                    '<td class="params-cell">' + optionsHtml + '</td>' +
                    '</tr>'
                );
            });

            tbody.innerHTML = rows.join('');

            // Add click handlers
            tbody.querySelectorAll('tr').forEach(function(row) {
                row.addEventListener('click', function() {
                    var id = this.dataset.id;
                    selectTask(id);
                    tbody.querySelectorAll('tr').forEach(function(r) { r.classList.remove('selected'); });
                    this.classList.add('selected');
                });
            });
        }

        // Table sorting
        document.querySelectorAll('#taskTable th[data-sort]').forEach(function(th) {
            th.addEventListener('click', function() {
                var col = this.dataset.sort;
                if (sortColumn === col) {
                    sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    sortColumn = col;
                    sortDirection = 'asc';
                }

                // Update sort indicators
                document.querySelectorAll('#taskTable th').forEach(function(h) {
                    h.classList.remove('sorted');
                    var icon = h.querySelector('.sort-icon');
                    if (icon) icon.innerHTML = '&#8597;';
                });
                this.classList.add('sorted');
                var icon = this.querySelector('.sort-icon');
                if (icon) icon.innerHTML = sortDirection === 'asc' ? '&#8593;' : '&#8595;';

                renderTable();
            });
        });

        // Task selection (network click)
        network.on('click', function(params) {
            if (params.nodes.length > 0) {
                selectTask(params.nodes[0]);
            }
        });

        function selectTask(taskId) {
            selectedTaskId = taskId;
            var node = nodesData.find(function(n) { return n.id === taskId; });
            if (!node) return;

            // Show details
            document.getElementById('placeholder-message').classList.add('hidden');
            document.getElementById('task-details').classList.remove('hidden');

            document.getElementById('detail-id').textContent = node.id;
            document.getElementById('detail-process').textContent = node.process;
            document.getElementById('detail-instance').textContent = node.instance || '-';

            var stageEl = document.getElementById('detail-stage');
            stageEl.textContent = node.stage;
            stageEl.style.backgroundColor = stageColors[node.stage] || stageColors['default'];

            // Predecessors
            var predList = document.getElementById('detail-predecessors');
            var noPred = document.getElementById('detail-no-predecessors');
            if (node.predecessors && node.predecessors.length > 0) {
                predList.innerHTML = node.predecessors.map(function(p) {
                    return '<li>' + p + '</li>';
                }).join('');
                predList.style.display = 'block';
                noPred.style.display = 'none';
            } else {
                predList.style.display = 'none';
                noPred.style.display = 'block';
            }

            // Parameters
            var paramsSection = document.getElementById('params-section');
            var paramsTable = document.getElementById('detail-params');
            if (node.parameters && Object.keys(node.parameters).length > 0) {
                paramsTable.innerHTML = Object.entries(node.parameters).map(function(p) {
                    return '<tr><td>' + p[0] + '</td><td>' + p[1] + '</td></tr>';
                }).join('');
                paramsSection.style.display = 'block';
            } else {
                paramsSection.style.display = 'none';
            }

            // Execution Options
            var optionsSection = document.getElementById('options-section');
            var optionsTable = document.getElementById('detail-options');
            var optionRows = [];

            if (node.timeout !== null && node.timeout !== undefined) {
                optionRows.push('<tr><td>Timeout</td><td>' + node.timeout + ' seconds</td></tr>');
            }
            if (node.cancel_at_timeout) {
                optionRows.push('<tr><td>Cancel at Timeout</td><td>Yes</td></tr>');
            }
            if (node.safe_retry) {
                optionRows.push('<tr><td>Safe Retry</td><td>Yes</td></tr>');
            }
            if (!node.require_predecessor_success) {
                optionRows.push('<tr><td>Require Predecessor Success</td><td>No</td></tr>');
            }
            if (node.succeed_on_minor_errors) {
                optionRows.push('<tr><td>Succeed on Minor Errors</td><td>Yes</td></tr>');
            }

            if (optionRows.length > 0) {
                optionsTable.innerHTML = optionRows.join('');
                optionsSection.style.display = 'block';
            } else {
                optionsSection.style.display = 'none';
            }

            // Highlight in network
            network.selectNodes([taskId]);
        }

        // Sidebar toggle
        function toggleSidebar() {
            var sidebar = document.getElementById('sidebar');
            var btn = document.getElementById('btnToggleSidebar');
            sidebar.classList.toggle('collapsed');
            var isCollapsed = sidebar.classList.contains('collapsed');
            btn.innerHTML = isCollapsed ? 'Details &lt;&lt;' : '&gt;&gt; Details';
        }

        // DAG controls
        function togglePhysics() {
            physicsEnabled = !physicsEnabled;
            network.setOptions({ physics: { enabled: physicsEnabled } });
            document.getElementById('btnPhysics').classList.toggle('active', physicsEnabled);
        }

        function zoomIn() {
            var scale = network.getScale();
            network.moveTo({ scale: scale * 1.3 });
        }

        function zoomOut() {
            var scale = network.getScale();
            network.moveTo({ scale: scale / 1.3 });
        }

        // Initial fit
        network.once("afterDrawing", function() {
            network.fit();
        });
    </script>
</body>
</html>
"""
