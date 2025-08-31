const fs = require('fs').promises;
const path = require('path');
const http = require('http');
const dotenv = require('dotenv');
const { w3cwebsocket } = require('websocket');

// Load environment variables
dotenv.config();
const RPC_URL = process.env.WRPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const MINING_ADDR = process.env.MINING_ADDR;

// Set WebSocket for vecno.js
globalThis.WebSocket = w3cwebsocket;

async function init() {
    let vecno, RpcClient, privateKey;
    try {
        // Load vecno.js and WASM module
        vecno = require('./vecno.js');
        const wasmPath = path.resolve(__dirname, 'vecno_bg.wasm');
        const wasmBuffer = await fs.readFile(wasmPath);
        const imports = { '__wbindgen_placeholder__': vecno };
        const wasmModule = await WebAssembly.compile(wasmBuffer);
        const wasmInstance = await WebAssembly.instantiate(wasmModule, imports);
        vecno.wasm = wasmInstance.exports;
        vecno.initWASM32Bindings({});

        // Initialize RpcClient
        RpcClient = vecno.RpcClient;
        if (!RpcClient) {
            throw new Error('RpcClient not found in vecno.js');
        }

        // Derive private key from xprv: m/44'/111111'/0'/0/0
        const xprv = vecno.XPrv.fromXPrv(PRIVATE_KEY);
        privateKey = xprv
            .deriveChild(44, true)
            .deriveChild(111111, true)
            .deriveChild(0, true)
            .deriveChild(0, false)
            .deriveChild(0, false)
            .toPrivateKey();

        // Validate MINING_ADDR
        const derivedAddress = privateKey.toAddress('mainnet').toString();
        if (derivedAddress !== MINING_ADDR) {
            throw new Error(`Derived address ${derivedAddress} does not match MINING_ADDR ${MINING_ADDR}`);
        }
    } catch (err) {
        throw new Error(`Initialization failed: ${err.message}`);
    }

    // Create RpcClient instance
    let rpcClient;
    try {
        rpcClient = new RpcClient({ url: RPC_URL });
        await rpcClient.connect();
    } catch (err) {
        throw new Error(`Failed to connect to Kaspa RPC: ${err.message}`);
    }

    // HTTP server
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
                    if (from !== MINING_ADDR) {
                        throw new Error(`From address ${from} does not match MINING_ADDR ${MINING_ADDR}`);
                    }
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
                    let pendingTx = vecno.createTransaction(utxo_entry_source, outputs, priority_fee, null, null);
                    pendingTx = vecno.signTransaction(pendingTx, [privateKey]);
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
        if (rpcClient) await rpcClient.disconnect();
        process.exit(0);
    });
}

init().catch(err => {
    console.error('Initialization failed:', err);
    process.exit(1);
});