/**
 * Loading Screen Component
 * Displays database download progress
 */

export class LoadingScreen {
    constructor() {
        this.elements = {
            overlay: document.getElementById('loading-overlay'),
            corpusBar: document.getElementById('corpus-progress-bar'),
            corpusPercent: document.getElementById('corpus-percent'),
            corpusSize: document.getElementById('corpus-size'),
            corpusStatus: document.getElementById('corpus-status'),
            lexiconBar: document.getElementById('lexicon-progress-bar'),
            lexiconPercent: document.getElementById('lexicon-percent'),
            lexiconSize: document.getElementById('lexicon-size'),
            lexiconStatus: document.getElementById('lexicon-status')
        };

        this.startTime = Date.now();
        this.lastUpdate = {};
        this.dbStatus = {
            corpus: null,
            lexicon: null
        };
        this.initializingShown = false;

        this.setupEventListeners();
    }

    setupEventListeners() {
        // Listen for database progress events
        window.addEventListener('db-progress', (e) => {
            this.updateProgress(e.detail);
        });

        // Listen for initialization progress events
        window.addEventListener('db-init-progress', (e) => {
            const { step, totalSteps, message } = e.detail;
            this.updateInitProgress(step, totalSteps, message);
        });

        // Listen for database ready event
        window.addEventListener('db-ready', () => {
            this.hide();
        });
    }

    updateProgress(detail) {
        const { dbName, state } = detail;
        const { status, progress, downloaded, total } = state;

        // Track status
        this.dbStatus[dbName] = status;

        // Update progress bar
        const progressBar = this.elements[`${dbName}Bar`];
        if (progressBar) {
            progressBar.style.width = `${progress}%`;
        }

        // Update percentage text
        const percentText = this.elements[`${dbName}Percent`];
        if (percentText) {
            percentText.textContent = `${Math.round(progress)}%`;
        }

        // Update size text
        const sizeText = this.elements[`${dbName}Size`];
        if (sizeText) {
            if (downloaded && total) {
                const downloadedMB = (downloaded / (1024 * 1024)).toFixed(1);
                const totalMB = (total / (1024 * 1024)).toFixed(1);
                sizeText.textContent = `${downloadedMB} / ${totalMB} MB`;
            } else {
                const totalMB = (total / (1024 * 1024)).toFixed(0);
                sizeText.textContent = `${totalMB} MB`;
            }
        }

        // Update status text
        const statusText = this.elements[`${dbName}Status`];
        if (statusText) {
            const statusMessage = this.getStatusMessage(status, dbName, downloaded, total);
            statusText.textContent = statusMessage;
        }

        // Track timing for speed calculation
        this.lastUpdate[dbName] = {
            time: Date.now(),
            downloaded
        };

        // Check if both databases are complete
        this.checkIfBothComplete();
    }

    checkIfBothComplete() {
        // Don't show message multiple times
        if (this.initializingShown) return;

        // Check if both databases are loading from cache (status = 'loading')
        // This happens before DuckDB initialization starts
        const corpusLoading = this.dbStatus.corpus === 'loading';
        const lexiconLoading = this.dbStatus.lexicon === 'loading';

        console.log('Loading screen: checking status - corpus:', this.dbStatus.corpus, 'lexicon:', this.dbStatus.lexicon);

        if (corpusLoading && lexiconLoading) {
            console.log('Loading screen: Both databases loading from cache, showing initializing message');
            this.initializingShown = true;
            // Show initializing message immediately when loading from cache
            this.showInitializingMessage();
        }
    }

    showInitializingMessage() {
        const overlay = this.elements.overlay;
        if (!overlay) return;

        console.log('Loading screen: Displaying spinner...');

        // Replace content with initializing message
        overlay.innerHTML = `
            <div class="loading-content">
                <h2>KEMET Data Explorer</h2>
                <div class="initializing-message">
                    <div class="spinner"></div>
                    <p class="status-text" id="init-status">Initializing database engine...</p>
                    <div class="init-progress-container">
                        <div class="init-progress-bar" id="init-progress-bar"></div>
                    </div>
                    <p class="init-percent" id="init-percent">0%</p>
                </div>
            </div>
        `;

        // Mark the time when we started showing the spinner
        this.spinnerStartTime = Date.now();
    }

    updateInitProgress(step, totalSteps, message) {
        const progressBar = document.getElementById('init-progress-bar');
        const percentText = document.getElementById('init-percent');
        const statusText = document.getElementById('init-status');

        if (progressBar && percentText) {
            const percent = Math.round((step / totalSteps) * 100);
            progressBar.style.width = `${percent}%`;
            percentText.textContent = `${percent}%`;
        }

        if (statusText && message) {
            statusText.textContent = message;
        }
    }

    getStatusMessage(status, dbName, downloaded, total) {
        switch (status) {
            case 'pending':
                return 'Waiting...';

            case 'downloading':
                return this.getDownloadSpeed(dbName, downloaded);

            case 'loading':
                return 'Loading from cache...';

            case 'complete':
                return 'Ready';

            default:
                return status;
        }
    }

    getDownloadSpeed(dbName, downloaded) {
        const last = this.lastUpdate[dbName];
        if (!last || !downloaded) {
            return 'Downloading...';
        }

        const now = Date.now();
        const timeDiff = (now - last.time) / 1000; // seconds
        const sizeDiff = downloaded - last.downloaded; // bytes

        if (timeDiff < 1 || sizeDiff <= 0) {
            return 'Downloading...';
        }

        const speedBPS = sizeDiff / timeDiff;
        const speedMBPS = speedBPS / (1024 * 1024);

        if (speedMBPS < 1) {
            const speedKBPS = speedBPS / 1024;
            return `Downloading... (${speedKBPS.toFixed(0)} KB/s)`;
        } else {
            return `Downloading... (${speedMBPS.toFixed(1)} MB/s)`;
        }
    }

    hide() {
        if (!this.elements.overlay) return;

        const hideOverlay = () => {
            // Fade out
            this.elements.overlay.style.opacity = '0';
            setTimeout(() => {
                this.elements.overlay.style.display = 'none';
            }, 300);

            const totalTime = ((Date.now() - this.startTime) / 1000).toFixed(1);
            console.log(`Databases loaded in ${totalTime} seconds`);
        };

        // If spinner is showing, ensure it displays for at least 800ms
        if (this.spinnerStartTime) {
            const spinnerElapsed = Date.now() - this.spinnerStartTime;
            const minDisplayTime = 800; // milliseconds

            if (spinnerElapsed < minDisplayTime) {
                const remainingTime = minDisplayTime - spinnerElapsed;
                console.log(`Loading screen: Waiting ${remainingTime}ms before hiding...`);
                setTimeout(hideOverlay, remainingTime);
            } else {
                hideOverlay();
            }
        } else {
            hideOverlay();
        }
    }

    show() {
        if (this.elements.overlay) {
            this.elements.overlay.style.display = 'flex';
            this.elements.overlay.style.opacity = '1';
        }
    }
}

// Auto-initialize
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        new LoadingScreen();
    });
} else {
    new LoadingScreen();
}

export default LoadingScreen;
