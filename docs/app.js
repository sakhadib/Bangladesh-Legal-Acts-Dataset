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
    const datasetLoader = document.getElementById('dataset-loader');
    const tableContainer = document.getElementById('table-container');

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
        
        fetch(`https://huggingface.co/datasets/sakhadib/Bangladesh-Legal-Acts-Dataset/resolve/main/acts/${file}`)
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

    // Modern action functions
    function copyActLink(filename, title) {
        const detailsUrl = `${window.location.origin}${window.location.pathname}details/?act=${encodeURIComponent(filename)}`;
        navigator.clipboard.writeText(detailsUrl).then(() => {
            showToast(`Link copied for "${title}"`, 'success');
        }).catch(() => {
            showToast('Failed to copy link', 'error');
        });
    }

    function shareAct(filename, title) {
        const detailsUrl = `${window.location.origin}${window.location.pathname}details/?act=${encodeURIComponent(filename)}`;
        
        if (navigator.share) {
            navigator.share({
                title: `${title} - Bangladesh Legal Acts Dataset`,
                text: `Check out this legal act from the Bangladesh Legal Acts Dataset: ${title}`,
                url: detailsUrl
            }).catch(err => console.log('Error sharing:', err));
        } else {
            // Fallback to copying link
            copyActLink(filename, title);
        }
    }

    function showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `fixed top-4 right-4 z-50 px-6 py-3 rounded-lg text-white font-medium transition-all duration-300 transform translate-x-full max-w-sm`;
        
        if (type === 'success') {
            toast.classList.add('bg-green-600');
            toast.innerHTML = `<i class="fas fa-check mr-2"></i>${message}`;
        } else if (type === 'error') {
            toast.classList.add('bg-red-600');
            toast.innerHTML = `<i class="fas fa-times mr-2"></i>${message}`;
        } else {
            toast.classList.add('bg-blue-600');
            toast.innerHTML = `<i class="fas fa-info mr-2"></i>${message}`;
        }
        
        document.body.appendChild(toast);
        
        // Animate in
        setTimeout(() => {
            toast.classList.remove('translate-x-full');
        }, 100);
        
        // Animate out and remove
        setTimeout(() => {
            toast.classList.add('translate-x-full');
            setTimeout(() => {
                document.body.removeChild(toast);
            }, 300);
        }, 3000);
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

    // Load data from local JavaScript file (no fetch needed)
    function loadData() {
        try {
            // Check if the local data is available
            if (typeof bangladeshLegalActs === 'undefined' || typeof getAllActs !== 'function') {
                throw new Error('Local Bangladesh Legal Acts data not found. Please ensure bangladesh_legal_acts_data.js is loaded.');
            }
            
            // Show loader briefly for UX
            datasetLoader.classList.remove('hidden');
            tableContainer.classList.add('hidden');
            searchInput.disabled = true;
            
            // Use setTimeout to show the loader briefly, then load local data
            setTimeout(() => {
                // Convert the bangladeshLegalActs object to array format
                allActs = getAllActs().map(act => ({
                    act_title: act.title,
                    act_year: act.year,
                    act_no: act.actNo,
                    source_file: act.link.split('/').pop() // Extract filename from URL
                })).sort((a, b) => (b.act_year || 0) - (a.act_year || 0)); // Sort by year descending
                
                filteredActs = allActs;
                
                // Hide loader and show table
                datasetLoader.classList.add('hidden');
                tableContainer.classList.remove('hidden');
                searchInput.disabled = false;
                
                renderTable();
                
                console.log(`Loaded ${allActs.length} acts from local data`);
            }, 800); // Brief delay to show the loader animation
            
        } catch (error) {
            console.error("Failed to load dataset:", error);
            
            // Hide loader and show error
            datasetLoader.classList.add('hidden');
            tableContainer.classList.remove('hidden');
            searchInput.disabled = false;
            
            actsTableBody.innerHTML = `<tr><td colspan="4" class="text-center p-4 text-red-400">Failed to load data. ${error.message}</td></tr>`;
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
            row.className = 'border-b border-gray-800 hover:bg-gray-800/30 transition-colors group';
            row.innerHTML = `
                <td class="p-4 font-medium">${act.act_title || 'N/A'}</td>
                <td class="p-4">${act.act_year || 'N/A'}</td>
                <td class="p-4">${act.act_no || 'N/A'}</td>
                <td class="p-4">
                    <div class="flex items-center justify-end gap-2">
                        <a href="./details/?act=${encodeURIComponent(act.source_file)}" 
                           class="inline-flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-all duration-200 hover:scale-105 hover:shadow-lg hover:shadow-blue-500/25" 
                           title="View full details" 
                           aria-label="View details for ${act.act_title}">
                            <i class="fas fa-external-link-alt text-xs"></i>
                            <span class="hidden sm:inline">View</span>
                        </a>
                        <button class="view-json-btn inline-flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-gray-300 bg-gray-700 hover:bg-gray-600 rounded-lg transition-all duration-200 hover:scale-105 hover:shadow-lg" 
                                title="Preview JSON data" 
                                data-file="${act.source_file}" 
                                data-title="${act.act_title || 'Act Details'}">
                            <i class="fas fa-code text-xs"></i>
                            <span class="hidden sm:inline">JSON</span>
                        </button>
                        <div class="relative group/dropdown">
                            <button class="inline-flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-gray-300 bg-gray-700 hover:bg-gray-600 rounded-lg transition-all duration-200 hover:scale-105 hover:shadow-lg" 
                                    title="More options">
                                <i class="fas fa-ellipsis-v text-xs"></i>
                                <span class="hidden sm:inline">More</span>
                            </button>
                            <div class="absolute right-0 top-full mt-1 w-40 bg-gray-800 border border-gray-700 rounded-lg shadow-xl opacity-0 invisible group-hover/dropdown:opacity-100 group-hover/dropdown:visible transition-all duration-200 z-10">
                                <a href="https://huggingface.co/datasets/sakhadib/Bangladesh-Legal-Acts-Dataset/resolve/main/acts/${act.source_file}" 
                                   download 
                                   class="flex items-center gap-3 px-3 py-2 text-sm text-gray-300 hover:bg-gray-700 hover:text-white transition-colors first:rounded-t-lg">
                                    <i class="fas fa-download text-xs text-teal-400"></i>
                                    Download JSON
                                </a>
                                <button onclick="copyActLink('${act.source_file}', '${act.act_title?.replace(/'/g, "\\'")}'); event.stopPropagation();" 
                                        class="flex items-center gap-3 px-3 py-2 text-sm text-gray-300 hover:bg-gray-700 hover:text-white transition-colors w-full text-left">
                                    <i class="fas fa-link text-xs text-blue-400"></i>
                                    Copy Link
                                </button>
                                <button onclick="shareAct('${act.source_file}', '${act.act_title?.replace(/'/g, "\\'")}'); event.stopPropagation();" 
                                        class="flex items-center gap-3 px-3 py-2 text-sm text-gray-300 hover:bg-gray-700 hover:text-white transition-colors last:rounded-b-lg w-full text-left">
                                    <i class="fas fa-share text-xs text-green-400"></i>
                                    Share Act
                                </button>
                            </div>
                        </div>
                    </div>
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

    // Search functionality (enhanced with local data)
    searchInput.addEventListener('input', () => {
        const searchTerm = searchInput.value.toLowerCase();
        
        if (searchTerm.trim() === '') {
            // If search is empty, show all acts
            filteredActs = allActs;
        } else {
            // Use the utility function from bangladesh_legal_acts_data.js for title search
            // and also search by year and act number
            filteredActs = allActs.filter(act => {
                const title = (act.act_title || '').toLowerCase();
                const year = (act.act_year || '').toString();
                const actNo = (act.act_no || '').toString().toLowerCase();
                return title.includes(searchTerm) || 
                       year.includes(searchTerm) || 
                       actNo.includes(searchTerm);
            });
        }
        
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
