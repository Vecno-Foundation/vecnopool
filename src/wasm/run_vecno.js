const fs = require('fs').promises;
const path = require('path');
const http = require('http');
const dotenv = require('dotenv');
const { w3cwebsocket } = require('websocket');
const sqlite3 = require('sqlite3').verbose();

// Load environment variables
dotenv.config();
const RPC_URL = process.env.WRPC_URL || 'ws://127.0.0.1:8110';
const MNEMONIC = process.env.MNEMONIC || 'acoustic once under insane delay void exhaust fold click cup raw evolve love pottery alpha often put marble bullet rapid cupboard chair cover supply';
const MINING_ADDR = process.env.MINING_ADDR || 'vecno:qznlxm9v9c5lv8atuyd34h6vc9d9nstrhy93fuy49ta5a0rdzv4pg9grqy3w4';
const DB_PATH = process.env.DB_PATH || './pool.db';
const NETWORK_ID = process.env.NETWORK_ID || 'mainnet';

// Set WebSocket for vecno.js
globalThis.WebSocket = w3cwebsocket;

// Helper function to serialize objects with BigInt
function serializeBigInt(obj) {
    const seen = new WeakSet();
    return JSON.stringify(obj, (key, value) => {
        if (typeof value === 'bigint') return value.toString();
        if (typeof value === 'object' && value !== null) {
            if (seen.has(value)) return undefined;
            seen.add(value);
            if (value instanceof vecno.Address || value instanceof vecno.Transaction || value instanceof vecno.TransactionInput ||
                value instanceof vecno.UtxoEntries || value instanceof vecno.PrivateKey || value instanceof vecno.Mnemonic ||
                value instanceof vecno.XPrv) {
                return Object.fromEntries(
                    Object.entries(value).filter(([k, v]) => typeof v !== 'function' && !(v instanceof Buffer))
                );
            }
        }
        return value;
    }, 2);
}

// Helper function to estimate transaction serialized size
function transactionSerializedByteSize(tx) {
    let size = 0;
    size += 2; // Tx version (u16)
    size += 8; // Number of inputs (u64)
    size += tx.inputs.reduce((sum, input) => sum + transactionInputSerializedByteSize(input), 0);
    size += 8; // Number of outputs (u64)
    size += tx.outputs.reduce((sum, output) => sum + transactionOutputSerializedByteSize(output), 0);
    size += 8; // Lock time (u64)
    size += 20; // Subnetwork ID
    size += 8; // Gas (u64)
    size += 32; // Payload hash
    size += 8; // Payload length (u64)
    size += tx.payload.length;
    return size;
}

function transactionInputSerializedByteSize(input) {
    let size = 0;
    size += 36; // Outpoint (32 txid + 4 index)
    size += 8; // Signature script length (u64)
    size += input.signatureScript.length;
    size += 8; // Sequence (u64)
    return size;
}

function transactionOutputSerializedByteSize(output) {
    let size = 0;
    size += 8; // Value (u64)
    size += 2; // Script version (u16)
    size += 8; // Script length (u64)
    size += output.scriptPublicKey.script.length;
    return size;
}

// Database connection
async function getDbConnection() {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(DB_PATH, (err) => {
            if (err) {
                reject(new Error(`Failed to connect to database: ${err.message}`));
            } else {
                resolve(db);
            }
        });
    });
}

// Fetch balances from the database
async function fetchBalances(db) {
    return new Promise((resolve, reject) => {
        db.all('SELECT id, address, balance FROM balances WHERE balance >= 1000000000', [], (err, rows) => {
            if (err) {
                reject(new Error(`Failed to fetch balances: ${err.message}`));
            } else {
                const balances = rows.map(row => ({
                    id: row.id,
                    address: row.address,
                    balance: BigInt(row.balance)
                }));
                resolve(balances);
            }
        });
    });
}

// Update balance in the database
async function resetBalance(db, address) {
    return new Promise((resolve, reject) => {
        db.run('UPDATE balances SET balance = 0 WHERE address = ?', [address], (err) => {
            if (err) {
                reject(new Error(`Failed to reset balance for ${address}: ${err.message}`));
            } else {
                resolve();
            }
        });
    });
}

// Record payment in the database
async function recordPayment(db, wallet_address, amount, transaction_hash) {
    return new Promise((resolve, reject) => {
        const timestamp = new Date().toISOString();
        db.run(
            'INSERT INTO payments (wallet_address, amount, timestamp, transaction_hash) VALUES (?, ?, ?, ?)',
            [wallet_address, amount.toString(), timestamp, transaction_hash],
            (err) => {
                if (err) {
                    reject(new Error(`Failed to record payment for ${wallet_address}: ${err.message}`));
                } else {
                    resolve();
                }
            }
        );
    });
}

async function init() {
    let vecno, RpcClient, privateKey, rpcClient, db;
    try {
        // Load vecno.js and WASM module
        vecno = require('./vecno.js');
        const wasmPath = path.resolve(__dirname, 'vecno_bg.wasm');
        const wasmBuffer = await fs.readFile(wasmPath);
        const imports = { '__wbindgen_placeholder__': vecno }; // Exact same initialization as old code
        const wasmModule = await WebAssembly.compile(wasmBuffer);
        const wasmInstance = await WebAssembly.instantiate(wasmModule, imports);
        vecno.wasm = wasmInstance.exports;
        vecno.initWASM32Bindings({});

        // Initialize RpcClient
        RpcClient = vecno.RpcClient;
        if (!RpcClient) {
            throw new Error('RpcClient not found in vecno.js');
        }

        // Derive private key from mnemonic: m/44'/111111'/0'/0/0
        const mnemonic = new vecno.Mnemonic(MNEMONIC);
        const seed = mnemonic.toSeed();
        const xprv = new vecno.XPrv(seed);
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

        // Create RpcClient instance
        rpcClient = new RpcClient({ url: RPC_URL, encoding: 'borsh', networkId: NETWORK_ID });
        await rpcClient.connect();

        // Verify node sync status
        const { isSynced } = await rpcClient.getServerInfo();
        if (!isSynced) {
            throw new Error('Node is not synced. Please wait for the node to sync.');
        }

        // Initialize database
        db = await getDbConnection();
    } catch (err) {
        throw new Error(`Initialization failed: ${err.message}`);
    }

    // HTTP server
    const server = http.createServer(async (req, res) => {
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
            return;
        }

        if (req.url === '/createTransaction' && req.method === 'POST') {
            let body = '';
            req.on('data', chunk => body += chunk);
            req.on('end', async () => {
                try {
                    const { from, to, amount, network_id } = JSON.parse(body);
                    if (from !== MINING_ADDR) {
                        throw new Error(`From address ${from} does not match MINING_ADDR ${MINING_ADDR}`);
                    }

                    // Fetch UTXOs
                    const utxoResponse = await rpcClient.getUtxosByAddresses({ addresses: [from] });
                    const utxos = utxoResponse.entries || [];
                    if (!Array.isArray(utxos) || utxos.length === 0) {
                        throw new Error(`No UTXOs found for address: ${from}`);
                    }

                    // Filter for P2PK UTXOs
                    const selectedUtxos = utxos.filter(utxo =>
                        utxo.scriptPublicKey.script.length === 68 &&
                        utxo.scriptPublicKey.script.startsWith('20') &&
                        utxo.scriptPublicKey.script.endsWith('ac')
                    );
                    if (selectedUtxos.length === 0) {
                        throw new Error(`No P2PK UTXOs found for address: ${from}`);
                    }

                    // Calculate total input amount
                    const totalInputAmount = selectedUtxos.reduce((sum, utxo) => sum + BigInt(utxo.amount), 0n);
                    const minimumFee = 210000n; // 0.0021 VE
                    const requiredAmount = BigInt(amount) + minimumFee;
                    if (totalInputAmount < requiredAmount) {
                        throw new Error(`Insufficient funds: ${totalInputAmount} sompi available, ${requiredAmount} required`);
                    }

                    // Create transaction inputs
                    const utxoEntryList = [];
                    const inputs = selectedUtxos.map((utxo, sequence) => {
                        utxoEntryList.push(utxo);
                        return new vecno.TransactionInput({
                            previousOutpoint: utxo.outpoint,
                            signatureScript: [],
                            sequence,
                            sigOpCount: 1,
                            utxo
                        });
                    });

                    // Create transaction outputs
                    const destinationScriptPublicKey = vecno.payToAddressScript(to);
                    const outputs = [
                        {
                            value: BigInt(amount),
                            scriptPublicKey: destinationScriptPublicKey
                        }
                    ];
                    const changeAmount = totalInputAmount - BigInt(amount) - minimumFee;
                    if (changeAmount > 0) {
                        outputs.push({
                            value: changeAmount,
                            scriptPublicKey: utxoEntryList[0].scriptPublicKey
                        });
                    }

                    // Create transaction
                    const transaction = new vecno.Transaction({
                        inputs,
                        outputs,
                        lockTime: 0,
                        subnetworkId: new Array(20).fill(0),
                        version: 0,
                        gas: 0,
                        payload: [],
                    });

                    // Estimate transaction mass and fee
                    const storageMassParameter = 2.036e9;
                    const computeMass = transactionSerializedByteSize(transaction) + (66 * inputs.length);
                    const outputHarmonic = outputs.reduce((sum, out) => sum + (storageMassParameter / Number(out.value)), 0);
                    const inputArithmetic = Number(totalInputAmount) / inputs.length;
                    const storageMass = outputHarmonic > inputArithmetic ? outputHarmonic - inputArithmetic : 0;
                    const totalMass = Math.max(computeMass, storageMass);
                    const estimatedFee = Math.ceil((totalMass * 1000) / 1000) || 1000;
                    if (BigInt(estimatedFee) > minimumFee) {
                        throw new Error(`Estimated fee ${estimatedFee} exceeds minimum fee ${minimumFee}`);
                    }

                    // Verify public key matches UTXO
                    const publicKey = privateKey.toPublicKey();
                    const utxoPubKey = utxoEntryList[0].scriptPublicKey.script.slice(2, 66);
                    const derivedPubKey = publicKey.toString().slice(2);
                    if (utxoPubKey !== derivedPubKey) {
                        throw new Error("Public key does not match UTXO scriptPublicKey!");
                    }

                    // Sign transaction with Schnorr
                    const signedTransaction = vecno.signTransaction(transaction, [privateKey], true);
                    if (signedTransaction.inputs.some(input => !input.signatureScript || input.signatureScript.length === 0)) {
                        throw new Error("Signature script is empty after signing!");
                    }

                    // Serialize transaction
                    const result = signedTransaction.serializeToJSON();
                    res.writeHead(200);
                    res.end(JSON.stringify({ result }));
                } catch (err) {
                    res.writeHead(500);
                    res.end(JSON.stringify({ error: err.message }));
                }
            });
            return;
        }

        if (req.url === '/submitTransaction' && req.method === 'POST') {
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
            return;
        }

        if (req.url === '/processPayouts' && req.method === 'POST') {
            try {
                const balances = await fetchBalances(db);
                const transactions = [];
                for (const { id, address, balance } of balances) {
                    // Fetch UTXOs
                    const utxoResponse = await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] });
                    const utxos = utxoResponse.entries || [];
                    if (!Array.isArray(utxos) || utxos.length === 0) {
                        console.warn(`No UTXOs found for address: ${MINING_ADDR}`);
                        continue;
                    }

                    // Filter for P2PK UTXOs
                    const selectedUtxos = utxos.filter(utxo =>
                        utxo.scriptPublicKey.script.length === 68 &&
                        utxo.scriptPublicKey.script.startsWith('20') &&
                        utxo.scriptPublicKey.script.endsWith('ac')
                    );
                    if (selectedUtxos.length === 0) {
                        console.warn(`No P2PK UTXOs found for address: ${MINING_ADDR}`);
                        continue;
                    }

                    // Calculate total input amount
                    const totalInputAmount = selectedUtxos.reduce((sum, utxo) => sum + BigInt(utxo.amount), 0n);
                    const minimumFee = 210000n; // 0.0021 VE
                    const requiredAmount = balance + minimumFee;
                    if (totalInputAmount < requiredAmount) {
                        console.warn(`Insufficient funds for ${address}: ${totalInputAmount} sompi available, ${requiredAmount} required`);
                        continue;
                    }

                    // Create transaction inputs
                    const utxoEntryList = [];
                    const inputs = selectedUtxos.map((utxo, sequence) => {
                        utxoEntryList.push(utxo);
                        return new vecno.TransactionInput({
                            previousOutpoint: utxo.outpoint,
                            signatureScript: [],
                            sequence,
                            sigOpCount: 1,
                            utxo
                        });
                    });

                    // Create transaction outputs
                    const destinationScriptPublicKey = vecno.payToAddressScript(address);
                    const outputs = [
                        {
                            value: balance,
                            scriptPublicKey: destinationScriptPublicKey
                        }
                    ];
                    const changeAmount = totalInputAmount - balance - minimumFee;
                    if (changeAmount > 0) {
                        outputs.push({
                            value: changeAmount,
                            scriptPublicKey: utxoEntryList[0].scriptPublicKey
                        });
                    }

                    // Create transaction
                    const transaction = new vecno.Transaction({
                        inputs,
                        outputs,
                        lockTime: 0,
                        subnetworkId: new Array(20).fill(0),
                        version: 0,
                        gas: 0,
                        payload: [],
                    });

                    // Estimate transaction mass and fee
                    const storageMassParameter = 2.036e9;
                    const computeMass = transactionSerializedByteSize(transaction) + (66 * inputs.length);
                    const outputHarmonic = outputs.reduce((sum, out) => sum + (storageMassParameter / Number(out.value)), 0);
                    const inputArithmetic = Number(totalInputAmount) / inputs.length;
                    const storageMass = outputHarmonic > inputArithmetic ? outputHarmonic - inputArithmetic : 0;
                    const totalMass = Math.max(computeMass, storageMass);
                    const estimatedFee = Math.ceil((totalMass * 1000) / 1000) || 1000;
                    if (BigInt(estimatedFee) > minimumFee) {
                        console.warn(`Skipping payout for ${address}: Estimated fee ${estimatedFee} exceeds minimum fee ${minimumFee}`);
                        continue;
                    }

                    // Verify public key matches UTXO
                    const publicKey = privateKey.toPublicKey();
                    const utxoPubKey = utxoEntryList[0].scriptPublicKey.script.slice(2, 66);
                    const derivedPubKey = publicKey.toString().slice(2);
                    if (utxoPubKey !== derivedPubKey) {
                        throw new Error(`Public key does not match UTXO scriptPublicKey for ${address}`);
                    }

                    // Sign transaction with Schnorr
                    const signedTransaction = vecno.signTransaction(transaction, [privateKey], true);
                    if (signedTransaction.inputs.some(input => !input.signatureScript || input.signatureScript.length === 0)) {
                        throw new Error(`Signature script is empty after signing for ${address}`);
                    }

                    // Submit transaction
                    const result = await signedTransaction.submit(rpcClient);
                    const txId = result.transactionId;

                    // Update database
                    await recordPayment(db, address, balance, txId);
                    await resetBalance(db, address);

                    transactions.push({ address, amount: balance, txId });
                }

                res.writeHead(200);
                res.end(JSON.stringify({ result: transactions }));
            } catch (err) {
                res.writeHead(500);
                res.end(JSON.stringify({ error: err.message }));
            }
            return;
        }

        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Not found' }));
    });

    server.listen(8181, () => console.log('WASM server running on port 8181'));

    // Cleanup on process exit
    process.on('SIGINT', async () => {
        if (rpcClient) await rpcClient.disconnect();
        db.close((err) => {
            if (err) console.error(`Failed to close database: ${err.message}`);
        });
        process.exit(0);
    });
}

init().catch(err => {
    console.error('Initialization failed:', err);
    process.exit(1);
});