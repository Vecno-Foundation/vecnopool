const fs = require('fs').promises;
const path = require('path');
const http = require('http');
const dotenv = require('dotenv');
const { w3cwebsocket } = require('websocket');
const sqlite3 = require('sqlite3').verbose();

// Load environment variables
dotenv.config();
const RPC_URL = process.env.WRPC_URL;
const MNEMONIC = process.env.MNEMONIC;
const MINING_ADDR = process.env.MINING_ADDR;
const DB_PATH = process.env.DB_PATH;
const NETWORK_ID = process.env.NETWORK_ID;

// Set WebSocket for vecno
globalThis.WebSocket = w3cwebsocket;

// Helper function to serialize objects with BigInt
function serializeBigInt(obj) {
    const seen = new WeakSet();
    return JSON.stringify(obj, (key, value) => {
        if (typeof value === 'bigint') return value.toString();
        if (typeof value === 'object' && value !== null) {
            if (seen.has(value)) return undefined;
            seen.add(value);
            if (typeof vecno !== 'undefined' && (
                value instanceof vecno.Address ||
                value instanceof vecno.Transaction ||
                value instanceof vecno.TransactionInput ||
                value instanceof vecno.UtxoEntries ||
                value instanceof vecno.PrivateKey ||
                value instanceof vecno.Mnemonic ||
                value instanceof vecno.XPrv
            )) {
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
    size += 8; // Script length (u64);
    size += output.scriptPublicKey.script.length;
    return size;
}

// Helper function to strip worker suffix from address
function stripWorkerSuffix(address) {
    const parts = address.split('.');
    return parts[0]; // Return base address without worker suffix
}

// Database connection
async function getDbConnection() {
    return new Promise((resolve, reject) => {
        const resolvedPath = path.resolve(DB_PATH);
        console.log("Connecting to database at:", resolvedPath);
        const db = new sqlite3.Database(resolvedPath, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE, (err) => {
            if (err) {
                reject(new Error(`Failed to connect to database at ${resolvedPath}: ${err.message}`));
            } else {
                db.run(
                    `CREATE TABLE IF NOT EXISTS balances (
                        id TEXT NOT NULL,
                        address TEXT NOT NULL,
                        available_balance INTEGER NOT NULL DEFAULT 0,
                        total_earned_balance INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(id, address)
                    )`,
                    (err) => {
                        if (err) {
                            reject(new Error(`Failed to create balances table: ${err.message}`));
                        } else {
                            console.log("Balances table ensured");
                            db.run(
                                `CREATE TABLE IF NOT EXISTS payments (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    address TEXT NOT NULL,
                                    amount INTEGER NOT NULL,
                                    tx_id TEXT NOT NULL,
                                    timestamp INTEGER NOT NULL
                                )`,
                                (err) => {
                                    if (err) {
                                        reject(new Error(`Failed to create payments table: ${err.message}`));
                                    } else {
                                        console.log("Payments table ensured");
                                        db.all("PRAGMA table_info(payments)", (err, rows) => {
                                            if (err) {
                                                console.error("Failed to check payments table schema:", err.message);
                                                reject(err);
                                            } else {
                                                const hasTxId = rows.some(row => row.name === 'tx_id');
                                                if (!hasTxId) {
                                                    console.log("Adding tx_id column to payments table...");
                                                    db.run("ALTER TABLE payments ADD COLUMN tx_id TEXT", (err) => {
                                                        if (err) {
                                                            console.error("Failed to add tx_id column:", err.message);
                                                            reject(err);
                                                        } else {
                                                            console.log("tx_id column added successfully");
                                                        }
                                                    });
                                                }
                                                db.all("SELECT name FROM sqlite_master WHERE type='table'", [], (err, rows) => {
                                                    if (err) {
                                                        console.error("Failed to list tables:", err.message);
                                                    } else {
                                                        console.log("Available tables:", rows.map(row => row.name));
                                                    }
                                                });
                                                resolve(db);
                                            }
                                        });
                                    }
                                }
                            );
                        }
                    }
                );
            }
        });
    });
}

// Fetch balances from the database
async function fetchBalances(db) {
    return new Promise((resolve, reject) => {
        const query = 'SELECT id, address, available_balance FROM balances WHERE available_balance >= 100000000';
        console.log("Executing query:", query);
        db.all(query, [], (err, rows) => {
            if (err) {
                console.error("Error fetching balances:", err.message);
                console.error("SQL Query:", query);
                console.error("Database Path:", path.resolve(DB_PATH));
                reject(new Error(`Failed to fetch balances: ${err.message}`));
            } else {
                console.log("Raw balances rows:", rows);
                const balances = rows.map(row => ({
                    id: row.id,
                    address: row.address,
                    balance: BigInt(row.available_balance)
                }));
                console.log("Processed balances:", serializeBigInt(balances));
                if (balances.length === 0) {
                    console.warn("No eligible balances found for payout (min_balance=100000000)");
                }
                resolve(balances);
            }
        });
    });
}

// Update balance in the database
async function resetBalance(db, address) {
    return new Promise((resolve, reject) => {
        db.run('UPDATE balances SET available_balance = 0 WHERE address = ?', [address], (err) => {
            if (err) {
                reject(new Error(`Failed to reset balance for ${address}: ${err.message}`));
            } else {
                console.log(`Balance reset for address: ${address}`);
                resolve();
            }
        });
    });
}

// Record payment in the database
async function recordPayment(db, wallet_address, amount, tx_id) {
    return new Promise((resolve, reject) => {
        const timestamp = Math.floor(Date.now() / 1000); // Unix timestamp in seconds
        db.run(
            'INSERT INTO payments (address, amount, tx_id, timestamp) VALUES (?, ?, ?, ?)',
            [wallet_address, amount.toString(), tx_id, timestamp],
            (err) => {
                if (err) {
                    reject(new Error(`Failed to record payment for ${wallet_address}: ${err.message}`));
                } else {
                    console.log(`Payment recorded: address=${wallet_address}, amount=${amount}, tx_id=${tx_id}`);
                    resolve();
                }
            }
        );
    });
}

async function init() {
    let vecno, RpcClient, Address, PrivateKey, Mnemonic, XPrv, NetworkType, sompiToVecnoStringWithSuffix, UtxoProcessor, UtxoContext, createTransactions, privateKey, rpcClient, db, processor, context;
    try {
        console.log("Loading vecno module from ../wasm...");
        try {
            vecno = require('../wasm');
            if (!vecno) {
                throw new Error('vecno module is undefined or empty');
            }
            console.log("vecno module loaded successfully");
            console.log("vecno exports:", Object.keys(vecno));
        } catch (err) {
            throw new Error(`Failed to load vecno module: ${err.message}`);
        }

        ({ RpcClient, Address, PrivateKey, Mnemonic, XPrv, NetworkType, sompiToVecnoStringWithSuffix, UtxoProcessor, UtxoContext, createTransactions } = vecno);
        if (!RpcClient) {
            throw new Error('RpcClient not found in vecno module');
        }
        if (!createTransactions) {
            throw new Error('createTransactions not found in vecno module');
        }
        console.log("vecno functions extracted successfully");

        console.log("MINING_ADDR loaded from env:", MINING_ADDR);
        const mnemonic = new Mnemonic(MNEMONIC);
        const seed = mnemonic.toSeed();
        const xprv = new XPrv(seed);
        privateKey = xprv
            .deriveChild(44, true)
            .deriveChild(111111, true)
            .deriveChild(0, true)
            .deriveChild(0, false)
            .deriveChild(0, false)
            .toPrivateKey();

        const derivedAddress = privateKey.toAddress('mainnet').toString();
        if (derivedAddress !== MINING_ADDR) {
            throw new Error(`Derived address ${derivedAddress} does not match MINING_ADDR ${MINING_ADDR}`);
        }

        rpcClient = new RpcClient({ url: RPC_URL, encoding: 'borsh', networkId: NETWORK_ID });
        await rpcClient.connect();
        console.log("RpcClient connected");

        const { isSynced } = await rpcClient.getServerInfo();
        if (!isSynced) {
            throw new Error('Node is not synced. Please wait for the node to sync.');
        }
        console.log("Node is synced");

        db = await getDbConnection();

        processor = new UtxoProcessor({ rpc: rpcClient, networkId: NETWORK_ID });
        context = new UtxoContext({ processor });
        processor.addEventListener("utxo-proc-start", async () => {
            console.log("Clearing UtxoContext and tracking address...");
            await context.clear();
            await context.trackAddresses([MINING_ADDR]);
            console.log("UtxoContext tracking initialized");
        });
        console.log("Starting UtxoProcessor...");
        await processor.start();
        console.log("UtxoProcessor started");

        console.log("Waiting for UTXO processor to initialize...");
        await new Promise(resolve => setTimeout(resolve, 5000));

        const server = http.createServer(async (req, res) => {
            res.setHeader('Content-Type', 'application/json');

            console.log("vecno state before request:", typeof vecno, Object.keys(vecno));

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
                        const result = sompiToVecnoStringWithSuffix(sompi, network_id);
                        res.writeHead(200);
                        res.end(JSON.stringify({ result }));
                    } catch (err) {
                        if (!res.headersSent) {
                            res.writeHead(500);
                            res.end(JSON.stringify({ error: err.message }));
                        } else {
                            console.error("Cannot send error response, headers already sent:", err.message);
                        }
                    }
                });
                return;
            }

            if (req.url === '/createTransaction' && req.method === 'POST') {
                let body = '';
                req.on('data', chunk => body += chunk);
                req.on('end', async () => {
                    try {
                        if (!vecno) {
                            throw new Error('vecno module is not defined');
                        }
                        const { from, to, amount, network_id } = JSON.parse(body);
                        if (from !== MINING_ADDR) {
                            throw new Error(`From address ${from} does not match MINING_ADDR ${MINING_ADDR}`);
                        }

                        const cleanTo = stripWorkerSuffix(to);
                        console.log(`Creating transaction: from=${from}, to=${cleanTo}, amount=${amount}`);

                        const utxoResponse = await rpcClient.getUtxosByAddresses({ addresses: [from] });
                        console.log("UTXOs retrieved:", serializeBigInt(utxoResponse.entries || []));
                        const utxos = utxoResponse.entries || [];
                        if (!Array.isArray(utxos) || utxos.length === 0) {
                            console.error(`No UTXOs found for address: ${from}`);
                            throw new Error(`No UTXOs found for address: ${from}`);
                        }

                        const selectedUtxos = utxos.filter(utxo =>
                            utxo.scriptPublicKey.script.length === 68 &&
                            utxo.scriptPublicKey.script.startsWith('20') &&
                            utxo.scriptPublicKey.script.endsWith('ac')
                        );
                        console.log("Selected P2PK UTXOs:", serializeBigInt(selectedUtxos));
                        if (selectedUtxos.length === 0) {
                            console.error(`No P2PK UTXOs found for address: ${from}`);
                            throw new Error(`No P2PK UTXOs found for address: ${from}`);
                        }

                        const totalInputAmount = selectedUtxos.reduce((sum, utxo) => sum + BigInt(utxo.amount), 0n);
                        console.log(`Total input amount: ${totalInputAmount} sompi`);
                        const minimumFee = 210000n; // 0.0021 VE
                        const requiredAmount = BigInt(amount) + minimumFee;
                        if (totalInputAmount < requiredAmount) {
                            console.error(`Insufficient funds: ${totalInputAmount} sompi available, ${requiredAmount} required`);
                            throw new Error(`Insufficient funds: ${totalInputAmount} sompi available, ${requiredAmount} required`);
                        }

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
                        console.log("Transaction inputs:", serializeBigInt(inputs));

                        const destinationScriptPublicKey = vecno.payToAddressScript(cleanTo);
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
                        console.log("Transaction outputs:", serializeBigInt(outputs));

                        const transaction = new vecno.Transaction({
                            inputs,
                            outputs,
                            lockTime: 0,
                            subnetworkId: new Array(20).fill(0),
                            version: 0,
                            gas: 0,
                            payload: [],
                        });

                        const storageMassParameter = 2.036e9;
                        const computeMass = transactionSerializedByteSize(transaction) + (66 * inputs.length);
                        const outputHarmonic = outputs.reduce((sum, out) => sum + (storageMassParameter / Number(out.value)), 0);
                        const inputArithmetic = Number(totalInputAmount) / inputs.length;
                        const storageMass = outputHarmonic > inputArithmetic ? outputHarmonic - inputArithmetic : 0;
                        const totalMass = Math.max(computeMass, storageMass);
                        const estimatedFee = Math.ceil((totalMass * 1000) / 1000) || 1000;
                        console.log(`Estimated fee: ${estimatedFee}, Minimum fee: ${minimumFee}`);
                        if (BigInt(estimatedFee) > minimumFee) {
                            console.error(`Estimated fee ${estimatedFee} exceeds minimum fee ${minimumFee}`);
                            throw new Error(`Estimated fee ${estimatedFee} exceeds minimum fee ${minimumFee}`);
                        }

                        const publicKey = privateKey.toPublicKey();
                        const utxoPubKey = utxoEntryList[0].scriptPublicKey.script.slice(2, 66);
                        const derivedPubKey = publicKey.toString().slice(2);
                        if (utxoPubKey !== derivedPubKey) {
                            console.error("Public key does not match UTXO scriptPublicKey!");
                            throw new Error("Public key does not match UTXO scriptPublicKey!");
                        }

                        const signedTransaction = vecno.signTransaction(transaction, [privateKey], true);
                        if (signedTransaction.inputs.some(input => !input.signatureScript || input.signatureScript.length === 0)) {
                            console.error("Signature script is empty after signing!");
                            throw new Error("Signature script is empty after signing!");
                        }

                        const result = signedTransaction.serializeToJSON();
                        console.log("Signed transaction:", result);
                        res.writeHead(200);
                        res.end(JSON.stringify({ result }));
                    } catch (err) {
                        console.error("Error in /createTransaction:", err.message);
                        console.error("Stack trace:", err.stack);
                        if (!res.headersSent) {
                            res.writeHead(500);
                            res.end(JSON.stringify({ error: err.message }));
                        } else {
                            console.error("Cannot send error response, headers already sent:", err.message);
                        }
                    }
                });
                return;
            }

            if (req.url === '/submitTransaction' && req.method === 'POST') {
                let body = '';
                req.on('data', chunk => body += chunk);
                req.on('end', async () => {
                    try {
                        if (!vecno) {
                            throw new Error('vecno module is not defined');
                        }
                        const { tx } = JSON.parse(body);
                        const pendingTx = vecno.PendingTransaction.deserializeFromJSON(tx);
                        const result = await pendingTx.submit(rpcClient);
                        console.log("Transaction submitted:", serializeBigInt(result));
                        res.writeHead(200);
                        res.end(JSON.stringify({ result }));
                    } catch (err) {
                        console.error("Error in /submitTransaction:", err.message);
                        console.error("Stack trace:", err.stack);
                        if (!res.headersSent) {
                            res.writeHead(500);
                            res.end(JSON.stringify({ error: err.message }));
                        } else {
                            console.error("Cannot send error response, headers already sent:", err.message);
                        }
                    }
                });
                return;
            }

            if (req.url === '/processPayouts' && req.method === 'POST') {
                try {
                    console.log("Processing /processPayouts request");
                    if (!vecno) {
                        throw new Error('vecno module is not defined');
                    }
                    if (!createTransactions) {
                        throw new Error('createTransactions function is not defined');
                    }

                    console.log("Fetching balances for payout...");
                    const balances = await fetchBalances(db);
                    console.log("Balances fetched:", serializeBigInt(balances));
                    if (balances.length === 0) {
                        console.warn("No balances eligible for payout: min_balance=100000000");
                        res.writeHead(200);
                        res.end(JSON.stringify({ result: [] }));
                        return;
                    }

                    console.log("MINING_ADDR:", MINING_ADDR);
                    const payments = balances
                        .filter(({ address }) => {
                            const cleanAddress = stripWorkerSuffix(address);
                            const isPoolAddress = cleanAddress === MINING_ADDR;
                            console.log(`Checking address: ${address}, cleanAddress: ${cleanAddress}, isPoolAddress: ${isPoolAddress}`);
                            return !isPoolAddress;
                        })
                        .map(({ address, balance }) => ({
                            address,
                            cleanAddress: stripWorkerSuffix(address),
                            amount: balance
                        }));

                    console.log("Payments after filtering:", serializeBigInt(payments));
                    if (payments.length === 0) {
                        console.warn("No eligible payouts after filtering pool address");
                        res.writeHead(200);
                        res.end(JSON.stringify({ result: [] }));
                        return;
                    }

                    console.log("Payments to process:", serializeBigInt(payments.map(p => ({
                        address: p.address,
                        cleanAddress: p.cleanAddress,
                        amount: sompiToVecnoStringWithSuffix(p.amount, NETWORK_ID)
                    }))));

                    console.log("Fetching UTXOs for address:", MINING_ADDR);
                    const utxoResponse = await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] });
                    console.log("UTXOs retrieved:", serializeBigInt(utxoResponse.entries || []));
                    const utxos = utxoResponse.entries || [];
                    if (!Array.isArray(utxos) || utxos.length === 0) {
                        console.error(`No UTXOs found for address: ${MINING_ADDR}`);
                        res.writeHead(500);
                        res.end(JSON.stringify({ error: `No UTXOs found for address: ${MINING_ADDR}` }));
                        return;
                    }

                    console.log("Creating transactions...");
                    let transactions, summary;
                    try {
                        ({ transactions, summary } = await createTransactions({
                            entries: context,
                            outputs: payments.map(p => ({
                                address: p.cleanAddress,
                                amount: p.amount
                            })),
                            changeAddress: MINING_ADDR,
                            priorityFee: 0n
                        }));
                    } catch (txError) {
                        console.error(`Failed to create transactions: ${txError.message}`);
                        console.error("Transaction error stack:", txError.stack);
                        res.writeHead(500);
                        res.end(JSON.stringify({ error: `Failed to create transactions: ${txError.message}` }));
                        return;
                    }

                    console.log("Transaction summary:", serializeBigInt(summary));
                    console.log(`Number of transactions: ${transactions.length}`);
                    console.log("Transactions:", serializeBigInt(transactions));

                    if (transactions.length === 0) {
                        console.error("No transactions created. Ensure the address has sufficient UTXOs.");
                        res.writeHead(500);
                        res.end(JSON.stringify({ error: "No transactions created. Ensure the address has sufficient UTXOs." }));
                        return;
                    }

                    const processedPayments = [];
                    for (const transaction of transactions) {
                        console.log(`Processing transaction ${transaction.id}...`);
                        if (!transaction.transaction || !Array.isArray(transaction.transaction.inputs)) {
                            console.error(`Invalid transaction structure for ID ${transaction.id}`);
                            res.writeHead(500);
                            res.end(JSON.stringify({ error: `Invalid transaction structure for ID ${transaction.id}` }));
                            return;
                        }
                        console.log(`Transaction inputs for ${transaction.id}:`, serializeBigInt(transaction.transaction.inputs));
                        console.log(`Signing transaction ${transaction.id}...`);
                        await transaction.sign([privateKey]);
                        if (!transaction.transaction.inputs.every(input => input.signatureScript && input.signatureScript.length > 0)) {
                            console.error(`Signature script is empty for transaction ${transaction.id}`);
                            res.writeHead(500);
                            res.end(JSON.stringify({ error: `Signature script is empty for transaction ${transaction.id}` }));
                            return;
                        }
                        console.log(`Submitting transaction ${transaction.id}...`);
                        const submissionResponse = await transaction.submit(rpcClient);
                        console.log(`Transaction ${transaction.id} submitted successfully. Response:`, serializeBigInt(submissionResponse));
                        for (const { address, amount } of payments) {
                            try {
                                await recordPayment(db, address, amount, transaction.id);
                                await resetBalance(db, address);
                                console.log(`Payout recorded: amount=${amount} sompi, address=${address}, tx_id=${transaction.id}`);
                                processedPayments.push({ address, amount: amount.toString(), txId: transaction.id });
                            } catch (dbError) {
                                console.warn(`Failed to record payment for ${address}: ${dbError.message}`);
                                continue;
                            }
                        }
                        await new Promise(resolve => setTimeout(resolve, 10000)); // 10-second delay
                    }

                    console.log("Raw summary object:", serializeBigInt(summary));
                    console.log("Final transaction ID:", summary.finalTransactionId || "undefined");
                    console.log("Total amount:", summary.finalAmount != null && typeof summary.finalAmount === 'bigint'
                        ? sompiToVecnoStringWithSuffix(summary.finalAmount, NETWORK_ID)
                        : "undefined");
                    console.log("Total fee:", summary.fees != null && typeof summary.fees === 'bigint'
                        ? sompiToVecnoStringWithSuffix(summary.fees, NETWORK_ID)
                        : "undefined");

                    res.writeHead(200);
                    res.end(serializeBigInt({ result: processedPayments }));
                } catch (err) {
                    console.error("Error processing payouts:", err.message);
                    console.error("Error stack:", err.stack);
                    res.writeHead(500);
                    res.end(JSON.stringify({ error: err.message }));
                }
                return;
            }

            res.writeHead(404);
            res.end(JSON.stringify({ error: 'Not found' }));
        });

        server.listen(8181, () => console.log('WASM server running on port 8181'));

        process.on('SIGINT', async () => {
            try {
                if (processor) {
                    console.log("Stopping UtxoProcessor...");
                    await processor.stop();
                    console.log("UtxoProcessor stopped");
                }
                if (rpcClient) {
                    console.log("Disconnecting RPC...");
                    await rpcClient.disconnect();
                    console.log("RPC disconnected");
                }
                if (db) {
                    await new Promise((resolve, reject) => {
                        db.close((err) => {
                            if (err) {
                                console.error(`Failed to close database: ${err.message}`);
                                reject(err);
                            } else {
                                console.log("Database closed");
                                resolve();
                            }
                        });
                    });
                }
            } catch (err) {
                console.error(`Cleanup error: ${err.message}`);
            }
            process.exit(0);
        });

    } catch (err) {
        console.error('Initialization failed:', err.message);
        console.error('Initialization error stack:', err.stack);
        process.exit(1);
    }
}

init();