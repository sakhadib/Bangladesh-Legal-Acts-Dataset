document.addEventListener('DOMContentLoaded', function() {
    const actsTableBody = document.getElementById('acts-table-body');
    const searchInput = document.getElementById('searchInput');
    const loadMoreBtn = document.getElementById('loadMoreBtn');
    const mobileMenuButton = document.getElementById('mobile-menu-button');
    const mobileMenu = document.getElementById('mobile-menu');
    const jsonViewerModal = document.getElementById('json-viewer-modal');
    const jsonViewerTitle = document.getElementById('json-viewer-title');
    const jsonViewerContent = document.getElementById('json-viewer-content');
    const jsonViewerClose = document.getElementById('json-viewer-close');

    let allActs = [];
    let filteredActs = [];
    let actsToShow = 15;
    const actsPerLoad = 15;

    // Toggle mobile menu
    mobileMenuButton.addEventListener('click', () => {
        mobileMenu.classList.toggle('hidden');
    });

    // --- JSON Viewer Modal Logic ---
    function openJsonViewer(file, title) {
        jsonViewerTitle.textContent = title;
        jsonViewerContent.textContent = 'Loading...';
        jsonViewerModal.classList.remove('hidden');
        
        fetch(`../Data/acts/${file}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                const formattedJson = JSON.stringify(data, null, 2);
                jsonViewerContent.textContent = formattedJson;
                hljs.highlightElement(jsonViewerContent);
            })
            .catch(error => {
                console.error("Failed to load act JSON:", error);
                jsonViewerContent.textContent = `Error loading file: ${error.message}`;
            });
    }

    function closeJsonViewer() {
        jsonViewerModal.classList.add('hidden');
    }

    jsonViewerClose.addEventListener('click', closeJsonViewer);
    jsonViewerModal.addEventListener('click', (e) => {
        if (e.target === jsonViewerModal) {
            closeJsonViewer();
        }
    });
    document.addEventListener('keydown', (e) => {
        if (e.key === "Escape" && !jsonViewerModal.classList.contains('hidden')) {
            closeJsonViewer();
        }
    });
    // --- End of JSON Viewer Modal Logic ---

    // Fetch and process data
    async function loadData() {
        try {
            const response = await fetch('../Data/Contextualized_Bangladesh_Legal_Acts.json');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            allActs = data.acts.sort((a, b) => (b.act_year || 0) - (a.act_year || 0)); // Sort by year descending
            filteredActs = allActs;
            renderTable();
        } catch (error) {
            console.error("Failed to load dataset:", error);
            actsTableBody.innerHTML = `<tr><td colspan="4" class="text-center p-4 text-red-400">Failed to load data. Please check the console.</td></tr>`;
        }
    }

    // Render table rows
    function renderTable() {
        actsTableBody.innerHTML = '';
        const actsToRender = filteredActs.slice(0, actsToShow);

        if (actsToRender.length === 0) {
            actsTableBody.innerHTML = `<tr><td colspan="4" class="text-center p-4">No acts found.</td></tr>`;
            loadMoreBtn.style.display = 'none';
            return;
        }

        actsToRender.forEach(act => {
            const row = document.createElement('tr');
            row.className = 'border-b border-gray-800 hover:bg-gray-800/50';
            row.innerHTML = `
                <td class="p-4 font-medium">${act.act_title || 'N/A'}</td>
                <td class="p-4">${act.act_year || 'N/A'}</td>
                <td class="p-4">${act.act_no || 'N/A'}</td>
                <td class="p-4 text-right">
                    <button class="view-json-btn text-teal-400 hover:text-teal-300 mr-4" title="View JSON" data-file="${act.source_file}" data-title="${act.act_title || 'Act Details'}">
                        <i class="fas fa-eye"></i>
                    </button>
                    <a href="../Data/acts/${act.source_file}" download class="text-teal-400 hover:text-teal-300" title="Download JSON">
                        <i class="fas fa-download"></i>
                    </a>
                </td>
            `;
            actsTableBody.appendChild(row);
        });

        // Add event listeners to new view buttons
        document.querySelectorAll('.view-json-btn').forEach(button => {
            button.addEventListener('click', (e) => {
                const file = e.currentTarget.dataset.file;
                const title = e.currentTarget.dataset.title;
                openJsonViewer(file, title);
            });
        });

        // Show/hide "Load More" button
        if (filteredActs.length > actsToShow) {
            loadMoreBtn.style.display = 'block';
        } else {
            loadMoreBtn.style.display = 'none';
        }
    }

    // Search functionality
    searchInput.addEventListener('input', () => {
        const searchTerm = searchInput.value.toLowerCase();
        filteredActs = allActs.filter(act => {
            const title = (act.act_title || '').toLowerCase();
            const year = (act.act_year || '').toString();
            const actNo = (act.act_no || '').toLowerCase();
            return title.includes(searchTerm) || year.includes(searchTerm) || actNo.includes(searchTerm);
        });
        actsToShow = actsPerLoad; // Reset display count
        renderTable();
    });

    // Load more functionality
    loadMoreBtn.addEventListener('click', () => {
        actsToShow += actsPerLoad;
        renderTable();
    });

    // Initial load
    loadData();
});
