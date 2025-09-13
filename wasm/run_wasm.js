const fs = require('fs').promises;
const path = require('path');
const dotenv = require('dotenv');
const { w3cwebsocket } = require('websocket');
const sqlite3 = require('sqlite3').verbose();

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, '../.env') });
const RPC_URL = process.env.WRPC_URL;
const MNEMONIC = process.env.MNEMONIC;
const MINING_ADDR = process.env.MINING_ADDR;
const DB_PATH = process.env.DB_PATH;
const NETWORK_ID = process.env.NETWORK_ID;

// Set WebSocket for vecno
globalThis.WebSocket = w3cwebsocket;

// Serialize objects with BigInt
function serializeBigInt(obj) {
  const seen = new WeakSet();
  return JSON.stringify(
    obj,
    (key, value) => {
      if (typeof value === 'bigint') return value.toString();
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return undefined;
        seen.add(value);
        if (
          typeof vecno !== 'undefined' &&
          (value instanceof vecno.Address ||
            value instanceof vecno.Transaction ||
            value instanceof vecno.TransactionInput ||
            value instanceof vecno.UtxoEntries ||
            value instanceof vecno.PrivateKey ||
            value instanceof vecno.Mnemonic ||
            value instanceof vecno.XPrv)
        ) {
          return Object.fromEntries(
            Object.entries(value).filter(([k, v]) => typeof v !== 'function' && !(v instanceof Buffer))
          );
        }
      }
      return value;
    },
    2
  );
}


// Strip worker suffix from address
function stripWorkerSuffix(address) {
  return address.split('.')[0];
}

// Database connection with indexing
async function getDbConnection() {
  const resolvedPath = path.resolve(DB_PATH);
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(resolvedPath, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE, (err) => {
      if (err) return reject(new Error(`Failed to connect to database: ${err.message}`));
      db.run(
        `CREATE TABLE IF NOT EXISTS balances (
          id TEXT NOT NULL,
          address TEXT NOT NULL,
          available_balance INTEGER NOT NULL DEFAULT 0,
          total_earned_balance INTEGER NOT NULL DEFAULT 0,
          UNIQUE(id, address)
        )`,
        (err) => {
          if (err) return reject(new Error(`Failed to create balances table: ${err.message}`));
          db.run(
            `CREATE INDEX IF NOT EXISTS idx_balances_available_balance ON balances (available_balance)`,
            (err) => {
              if (err) console.warn(`Failed to create index: ${err.message}`);
            }
          );
          db.run(
            `CREATE TABLE IF NOT EXISTS payments (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              address TEXT NOT NULL,
              amount INTEGER NOT NULL,
              tx_id TEXT NOT NULL,
              timestamp INTEGER NOT NULL
            )`,
            (err) => {
              if (err) return reject(new Error(`Failed to create payments table: ${err.message}`));
              db.all('PRAGMA table_info(payments)', (err, rows) => {
                if (err) return reject(err);
                if (!rows.some((row) => row.name === 'tx_id')) {
                  db.run('ALTER TABLE payments ADD COLUMN tx_id TEXT', (err) => {
                    if (err) console.warn(`Failed to add tx_id column: ${err.message}`);
                  });
                }
                resolve(db);
              });
            }
          );
        }
      );
    });
  });
}

// Fetch balances from database
async function fetchBalances(db) {
  const query = 'SELECT id, address, available_balance FROM balances WHERE available_balance >= 1000000000';
  return new Promise((resolve, reject) => {
    db.all(query, [], (err, rows) => {
      if (err) return reject(new Error(`Failed to fetch balances: ${err.message}`));
      const balances = rows.map((row) => ({
        id: row.id,
        address: row.address,
        balance: BigInt(row.available_balance),
      }));
      if (balances.length === 0) console.warn('No eligible balances for payout (min_balance = 10 VE)');
      resolve(balances);
    });
  });
}

// Reset balance in database
async function resetBalance(db, address) {
  return new Promise((resolve, reject) => {
    db.run('UPDATE balances SET available_balance = 0 WHERE address = ?', [address], (err) => {
      if (err) return reject(new Error(`Failed to reset balance for ${address}: ${err.message}`));
      resolve();
    });
  });
}

// Record payment in database
async function recordPayment(db, vecno, address, amount, tx_id, NETWORK_ID) {
  return new Promise((resolve, reject) => {
    const timestamp = Math.floor(Date.now() / 1000);
    db.run(
      'INSERT INTO payments (address, amount, tx_id, timestamp) VALUES (?, ?, ?, ?)',
      [address, amount.toString(), tx_id, timestamp],
      (err) => {
        if (err) return reject(new Error(`Failed to record payment for ${address}: ${err.message}`));
        console.log(`Payment recorded: address=${address}, amount=${vecno.sompiToVecnoStringWithSuffix(BigInt(amount), NETWORK_ID)}, tx_id=${tx_id}`);
        resolve();
      }
    );
  });
}

// Clean up old payments
async function cleanupOldPayments(db, retentionPeriodDays = 30) {
  const cutoffTimestamp = Math.floor(Date.now() / 1000) - retentionPeriodDays * 24 * 60 * 60;
  return new Promise((resolve, reject) => {
    db.run('DELETE FROM payments WHERE timestamp < ?', [cutoffTimestamp], (err) => {
      if (err) return reject(new Error(`Failed to clean up old payments: ${err.message}`));
      resolve();
    });
  });
}

// Process payouts
async function processPayouts(rpcClient, vecno, createTransactions, db, context, privateKey, MINING_ADDR, NETWORK_ID, processor) {
  try {
    if (!vecno || !createTransactions) throw new Error('Required modules undefined');

    const serverInfo = await rpcClient.getServerInfo();
    if (!serverInfo.isSynced) {
      throw new Error('Node not synced');
    }

    const balances = await fetchBalances(db);
    if (!balances.length) {
      return [];
    }

    const payments = balances
      .filter(({ address }) => stripWorkerSuffix(address) !== MINING_ADDR)
      .map(({ address, balance }, index) => ({
        address,
        cleanAddress: stripWorkerSuffix(address),
        amount: balance,
        paymentId: index,
      }));

    if (!payments.length) {
      console.warn('No eligible payouts after filtering');
      return [];
    }

    // Wait for UTXO processor
    await new Promise((resolve, reject) => {
      const maxWaitTime = 30000;
      let elapsed = 0;
      const checkUtxoReady = async () => {
        try {
          const utxos = (await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] })).entries || [];
          if (utxos.length) {
            resolve();
          } else if (elapsed >= maxWaitTime) {
            reject(new Error('No UTXOs available after timeout'));
          } else {
            elapsed += 1000;
            setTimeout(checkUtxoReady, 1000);
          }
        } catch (err) {
          reject(new Error(`UTXO check failed: ${err.message}`));
        }
      };
      checkUtxoReady();
    });

    const utxos = (await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] })).entries || [];
    if (!utxos.length) {
      console.error(`No UTXOs found for ${MINING_ADDR}`);
      try {
        await processor.stop();
        await processor.start();
        await context.clear();
        await context.trackAddresses([MINING_ADDR]);
      } catch (err) {
        console.error(`Failed to restart UtxoProcessor: ${err.message}`);
        return [];
      }
      const retryUtxos = (await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] })).entries || [];
      if (!retryUtxos.length) {
        console.error('Still no UTXOs available after restart');
        return [];
      }
    }

    const transactionOutputs = payments.map((p) => ({
      address: p.cleanAddress,
      amount: p.amount,
      paymentId: p.paymentId,
    }));

    const { transactions, summary } = await createTransactions({
      entries: context,
      outputs: transactionOutputs,
      changeAddress: MINING_ADDR,
      priorityFee: 0n,
    });

    if (!transactions.length) {
      console.error('No transactions created');
      return [];
    }

    const processedPayments = [];
    const processedPaymentIds = new Set();

    for (const transaction of transactions) {
      if (!transaction.transaction?.inputs?.length) {
        console.error(`Invalid transaction structure for ID ${transaction.id}`);
        continue;
      }

      await transaction.sign([privateKey]);
      if (!transaction.transaction.inputs.every((input) => input.signatureScript?.length)) {
        console.error(`Signature script empty for transaction ${transaction.id}`);
        continue;
      }

      const submissionResponse = await transaction.submit(rpcClient);
      console.log(`Transaction ${transaction.id} submitted: ${submissionResponse}`);

      for (const output of transaction.transaction.outputs || []) {
        const outputAmount = BigInt(output.value);
        const matchingPayment = payments.find(
          (p) => p.amount === outputAmount && !processedPaymentIds.has(p.paymentId)
        );

        if (matchingPayment) {
          await recordPayment(db, vecno, matchingPayment.address, matchingPayment.amount, transaction.id, NETWORK_ID);
          await resetBalance(db, matchingPayment.address);
          processedPayments.push({
            address: matchingPayment.address,
            amount: matchingPayment.amount.toString(),
            txId: transaction.id,
          });
          processedPaymentIds.add(matchingPayment.paymentId);
        }
      }
    }

    return processedPayments;
  } catch (err) {
    console.error(`Payout processing failed: ${err.message || 'Unknown error'}`);
    return [];
  }
}

// Initialize
async function init() {
  let vecno, RpcClient, PrivateKey, Mnemonic, XPrv, UtxoProcessor, UtxoContext, createTransactions;
  try {
    vecno = require('.');
    if (!vecno) throw new Error('vecno module undefined');
    ({ RpcClient, PrivateKey, Mnemonic, XPrv, UtxoProcessor, UtxoContext, createTransactions } = vecno);

    const mnemonic = new Mnemonic(MNEMONIC);
    const privateKey = new XPrv(mnemonic.toSeed())
      .deriveChild(44, true)
      .deriveChild(111111, true)
      .deriveChild(0, true)
      .deriveChild(0, false)
      .deriveChild(0, false)
      .toPrivateKey();

    if (privateKey.toAddress('mainnet').toString() !== MINING_ADDR) {
      throw new Error('Derived address does not match MINING_ADDR');
    }

    const rpcClient = new RpcClient({ url: RPC_URL, encoding: 'borsh', networkId: NETWORK_ID });
    await rpcClient.connect();
    if (!(await rpcClient.getServerInfo()).isSynced) {
      throw new Error('Node not synced');
    }

    const db = await getDbConnection();
    await cleanupOldPayments(db);

    const processor = new UtxoProcessor({ rpc: rpcClient, networkId: NETWORK_ID });
    const context = new UtxoContext({ processor });
    processor.addEventListener('utxo-proc-start', async () => {
      try {
        await context.clear();
        await context.trackAddresses([MINING_ADDR]);
      } catch (err) {
        console.error(`Failed to initialize UtxoContext: ${err.message}`);
      }
    });
    await processor.start();
    await new Promise((resolve) => setTimeout(resolve, 5000));

    // Schedule payouts every 10 minutes
    setInterval(
      async () => {
        const result = await processPayouts(rpcClient, vecno, createTransactions, db, context, privateKey, MINING_ADDR, NETWORK_ID, processor);
        console.log('Scheduled payout result:', serializeBigInt(result));
      },
      10 * 60 * 1000
    );

    // Initial payout
    const initialResult = await processPayouts(rpcClient, vecno, createTransactions, db, context, privateKey, MINING_ADDR, NETWORK_ID, processor);
    console.log('Initial payout result:', serializeBigInt(initialResult));

    // Cleanup on exit
    process.on('SIGINT', async () => {
      try {
        await processor?.stop();
        await rpcClient?.disconnect();
        await new Promise((resolve, reject) =>
          db.close((err) => (err ? reject(err) : resolve()))
        );
        console.log('Shutdown complete');
      } catch (err) {
        console.error(`Shutdown error: ${err.message}`);
      }
      process.exit(0);
    });
  } catch (err) {
    console.error(`Initialization failed: ${err.message}`);
    process.exit(1);
  }
}

init();