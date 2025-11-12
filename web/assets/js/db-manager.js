/**
 * Database Manager
 * Handles DuckDB-WASM loading, caching, and query execution
 */

import config from './config.js';

class DatabaseManager extends EventTarget {
    constructor() {
        super();
        this.db = null;
        this.isReady = false;
        this.state = {
            corpus: { status: 'pending', progress: 0, downloaded: 0, total: config.sizes.corpus },
            lexicon: { status: 'pending', progress: 0, downloaded: 0, total: config.sizes.lexicon }
        };
    }

    async initialize() {
        try {
            console.log('DatabaseManager: Starting initialization...');

            // Check cache first
            const corpusCached = await localforage.getItem(`corpus-${config.versions.corpus}`);
            const lexiconCached = await localforage.getItem(`lexicon-${config.versions.lexicon}`);

            if (corpusCached && lexiconCached) {
                console.log('DatabaseManager: Loading databases from cache...');
                await this.loadFromCache(corpusCached, lexiconCached);
            } else {
                console.log('DatabaseManager: Downloading databases...');
                await this.downloadAndCache();
            }

            console.log('DatabaseManager: Initialization complete!');
            this.isReady = true;
            this.dispatchEvent(new CustomEvent('ready'));
        } catch (error) {
            console.error('DatabaseManager: Failed to initialize:', error);
            this.dispatchEvent(new CustomEvent('error', { detail: error }));
            throw error;
        }
    }

    async downloadAndCache() {
        // Download lexicon first (smaller, loads faster)
        this.updateState('lexicon', 'downloading', 0);
        const lexiconBlob = await this.downloadWithProgress(
            config.databases.lexicon,
            (progress) => this.updateState('lexicon', 'downloading', progress)
        );
        await localforage.setItem(`lexicon-${config.versions.lexicon}`, lexiconBlob);
        this.updateState('lexicon', 'complete', 100);

        // Download corpus
        this.updateState('corpus', 'downloading', 0);
        const corpusBlob = await this.downloadWithProgress(
            config.databases.corpus,
            (progress) => this.updateState('corpus', 'downloading', progress)
        );
        await localforage.setItem(`corpus-${config.versions.corpus}`, corpusBlob);
        this.updateState('corpus', 'complete', 100);

        // Initialize DuckDB with downloaded files
        await this.initializeDuckDB(corpusBlob, lexiconBlob);
    }

    async loadFromCache(corpusBlob, lexiconBlob) {
        this.updateState('lexicon', 'loading', 100);
        this.updateState('corpus', 'loading', 100);

        // Give the browser a chance to render the spinner before starting heavy work
        // Use a small delay to allow DOM updates to be painted
        await new Promise(resolve => setTimeout(resolve, 50));

        await this.initializeDuckDB(corpusBlob, lexiconBlob);

        this.updateState('lexicon', 'complete', 100);
        this.updateState('corpus', 'complete', 100);
    }

    async downloadWithProgress(url, progressCallback) {
        // GitHub releases may redirect, so follow redirects and handle CORS
        const response = await fetch(url, {
            mode: 'cors',
            redirect: 'follow'
        });

        if (!response.ok) {
            throw new Error(`Failed to fetch ${url}: ${response.status} ${response.statusText}`);
        }

        const contentLength = response.headers.get('content-length');
        const total = parseInt(contentLength, 10);

        const reader = response.body.getReader();
        const chunks = [];
        let downloaded = 0;

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            chunks.push(value);
            downloaded += value.length;

            if (total) {
                const progress = (downloaded / total) * 100;
                progressCallback({ progress, downloaded, total });
            }
        }

        return new Blob(chunks);
    }

    dispatchInitProgress(step, totalSteps, message) {
        window.dispatchEvent(new CustomEvent('db-init-progress', {
            detail: { step, totalSteps, message }
        }));
    }

    async initializeDuckDB(corpusBlob, lexiconBlob) {
        const totalSteps = 7;
        let currentStep = 0;

        console.log('DatabaseManager: Initializing DuckDB-WASM...');
        console.log('Corpus blob size:', corpusBlob.size, 'bytes');
        console.log('Lexicon blob size:', lexiconBlob.size, 'bytes');

        // Step 1: Import DuckDB from CDN
        this.dispatchInitProgress(++currentStep, totalSteps, 'Loading DuckDB module...');
        console.log('DatabaseManager: Loading DuckDB from CDN...');
        const duckdb = await import('https://esm.sh/@duckdb/duckdb-wasm@1.30.0');

        // Build absolute URLs for Worker - relative paths don't work with Web Workers
        const baseUrl = new URL('./assets/lib/node_modules/@duckdb/duckdb-wasm/dist/', window.location.href);
        const bundle = {
            mainModule: new URL('duckdb-mvp.wasm', baseUrl).href,
            mainWorker: new URL('duckdb-browser-mvp.worker.js', baseUrl).href
        };

        // Step 2: Create worker
        this.dispatchInitProgress(++currentStep, totalSteps, 'Creating database worker...');
        console.log('DatabaseManager: Creating DuckDB worker from:', bundle.mainWorker);
        const logger = new duckdb.ConsoleLogger();
        const worker = new Worker(bundle.mainWorker);

        // Step 3: Instantiate DuckDB
        this.dispatchInitProgress(++currentStep, totalSteps, 'Instantiating DuckDB...');
        console.log('DatabaseManager: Instantiating DuckDB...');
        this.db = new duckdb.AsyncDuckDB(logger, worker);
        await this.db.instantiate(bundle.mainModule);

        // Step 4: Convert blobs to arrays
        this.dispatchInitProgress(++currentStep, totalSteps, 'Converting database files...');
        console.log('DatabaseManager: Converting blobs to arrays...');
        const corpusArray = new Uint8Array(await corpusBlob.arrayBuffer());
        const lexiconArray = new Uint8Array(await lexiconBlob.arrayBuffer());
        console.log('Corpus array length:', corpusArray.length);
        console.log('Lexicon array length:', lexiconArray.length);

        // Step 5: Register file buffers
        this.dispatchInitProgress(++currentStep, totalSteps, 'Registering database files...');
        console.log('DatabaseManager: Registering file buffers...');
        await this.db.registerFileBuffer('corpus.db', corpusArray);
        await this.db.registerFileBuffer('lexicon.db', lexiconArray);

        // Step 6: Create connection
        this.dispatchInitProgress(++currentStep, totalSteps, 'Creating database connection...');
        console.log('DatabaseManager: Creating connection...');
        this.conn = await this.db.connect();

        // Step 7: Attach databases
        this.dispatchInitProgress(++currentStep, totalSteps, 'Attaching databases...');
        console.log('DatabaseManager: Attaching databases...');
        await this.conn.query("ATTACH 'corpus.db' AS corpus");
        await this.conn.query("ATTACH 'lexicon.db' AS lexicon");

        console.log('DatabaseManager: DuckDB initialization complete!');
    }

    updateState(dbName, status, progress) {
        this.state[dbName] = {
            ...this.state[dbName],
            status,
            progress: typeof progress === 'number' ? progress :
                      typeof progress === 'object' ? (progress.downloaded / progress.total) * 100 : 0,
            downloaded: typeof progress === 'object' ? progress.downloaded : 0,
            total: typeof progress === 'object' ? progress.total : this.state[dbName].total
        };

        this.dispatchEvent(new CustomEvent('progress', {
            detail: { dbName, state: this.state[dbName] }
        }));
    }

    async query(sql, params = []) {
        if (!this.isReady) {
            throw new Error('Database not ready');
        }

        try {
            // Note: DuckDB-WASM doesn't support parameterized queries the same way
            // Queries should use direct string interpolation with proper escaping
            console.log('Executing query:', sql.substring(0, 100) + '...');
            const result = await this.conn.query(sql);
            const rows = result.toArray().map(row => Object.fromEntries(row));
            console.log('Query returned', rows.length, 'rows');
            return rows;
        } catch (error) {
            console.error('Query error:', error);
            throw error;
        }
    }
}

// Singleton instance
export const dbManager = new DatabaseManager();
export default dbManager;
