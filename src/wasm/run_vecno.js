const fs = require('fs').promises;
const path = require('path');
const http = require('http');
const dotenv = require('dotenv');
const { w3cwebsocket } = require('websocket');

// Load environment variables
dotenv.config();
const WRPC_URL = process.env.WRPC_URL || 'ws://127.0.0.1:8110';

// Set WebSocket for vecno.js
globalThis.WebSocket = w3cwebsocket;

async function init() {
    let vecno, RpcClient;
    try {
        // Load vecno.js
        vecno = require('./vecno.js');
        console.log('vecno.js exports:', Object.keys(vecno));

        // Initialize WASM module
        const wasmPath = path.resolve(__dirname, 'vecno_bg.wasm');
        const wasmBuffer = await fs.readFile(wasmPath);

        // Set up imports for wasm-bindgen
        const imports = {};
        imports['__wbindgen_placeholder__'] = vecno;

        // Instantiate WASM module
        const wasmModule = await WebAssembly.compile(wasmBuffer);
        const wasmInstance = await WebAssembly.instantiate(wasmModule, imports);

        // Assign wasm instance to global wasm variable
        vecno.wasm = wasmInstance.exports;
        console.log('WASM module initialized successfully');

        // Initialize WASM32 bindings
        if (typeof vecno.initWASM32Bindings === 'function') {
            vecno.initWASM32Bindings({});
            console.log('WASM32 bindings initialized');
        }

        // Initialize RpcClient
        RpcClient = vecno.RpcClient;
        if (!RpcClient) {
            throw new Error('RpcClient not found in vecno.js exports');
        }
    } catch (err) {
        console.error('WASM initialization failed:', err);
        throw err;
    }

    // Create RpcClient instance
    let rpcClient;
    try {
        rpcClient = new RpcClient({ url: WRPC_URL });
        await rpcClient.connect();
        console.log('Connected to Vecno RPC at', WRPC_URL);
    } catch (err) {
        console.error('Failed to connect to Vecno RPC:', err);
        throw err;
    }

    // Expose functions via HTTP server
    const server = http.createServer((req, res) => {
        res.setHeader('Content-Type', 'application/json');

        if (req.url === '/ping' && req.method === 'GET') {
            res.writeHead(200);
            res.end(JSON.stringify({ result: 'pong' }));
            return;
        }
        if (req.url === '/sompiToVecnoStringWithSuffix' && req.method === 'POST') {
            let body = '';
            req.on('data', chunk => body += chunk);
            req.on('end', () => {
                try {
                    const { sompi, network_id } = JSON.parse(body);
                    const result = vecno.sompiToVecnoStringWithSuffix(sompi, network_id);
                    res.writeHead(200);
                    res.end(JSON.stringify({ result }));
                } catch (err) {
                    res.writeHead(500);
                    res.end(JSON.stringify({ error: err.message }));
                }
            });
        } else if (req.url === '/createTransaction' && req.method === 'POST') {
            let body = '';
            req.on('data', chunk => body += chunk);
            req.on('end', async () => {
                try {
                    const { from, to, amount, network_id } = JSON.parse(body);
                    // Query UTXOs for the from address
                    const utxos = await rpcClient.getUtxosByAddresses([from]);
                    if (!utxos || utxos.length === 0) {
                        throw new Error(`No UTXOs found for address: ${from}`);
                    }
                    const utxo_entry_source = utxos.map(utxo => ({
                        address: from,
                        amount: utxo.amount,
                        outpoint: {
                            transactionId: utxo.transactionId,
                            index: utxo.index
                        }
                    }));
                    const outputs = [{ address: to, amount }];
                    const priority_fee = BigInt(0);
                    const pendingTx = vecno.createTransaction(utxo_entry_source, outputs, priority_fee, null, null);
                    const result = pendingTx.serializeToJSON();
                    res.writeHead(200);
                    res.end(JSON.stringify({ result }));
                } catch (err) {
                    res.writeHead(500);
                    res.end(JSON.stringify({ error: err.message }));
                }
            });
        } else if (req.url === '/submitTransaction' && req.method === 'POST') {
            let body = '';
            req.on('data', chunk => body += chunk);
            req.on('end', async () => {
                try {
                    const { tx } = JSON.parse(body);
                    const pendingTx = vecno.PendingTransaction.deserializeFromJSON(tx);
                    const result = await pendingTx.submit(rpcClient);
                    res.writeHead(200);
                    res.end(JSON.stringify({ result }));
                } catch (err) {
                    res.writeHead(500);
                    res.end(JSON.stringify({ error: err.message }));
                }
            });
        } else {
            res.writeHead(404);
            res.end(JSON.stringify({ error: 'Not found' }));
        }
    });

    server.listen(8181, () => console.log('WASM server running on port 8181'));

    // Cleanup on process exit
    process.on('SIGINT', async () => {
        if (rpcClient) {
            await rpcClient.disconnect();
            console.log('Disconnected from Vecno RPC');
        }
        process.exit(0);
    });
}

init().catch(err => {
    console.error('Initialization failed:', err);
    process.exit(1);
});