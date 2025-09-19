const fs = require('fs').promises;
const path = require('path');
const dotenv = require('dotenv');
const { w3cwebsocket } = require('websocket');
const { Pool } = require('pg');
const retry = require('async-retry');

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, '../.env') });
const RPC_URL = process.env.WRPC_URL;
const MNEMONIC = process.env.MNEMONIC;
const MINING_ADDR = process.env.MINING_ADDR;
const SQL_URI = process.env.SQL_URI;
const NETWORK_ID = process.env.NETWORK_ID;

// Validate environment variables
const requiredEnv = ['WRPC_URL', 'MNEMONIC', 'MINING_ADDR', 'SQL_URI', 'NETWORK_ID'];
for (const env of requiredEnv) {
  if (!process.env[env]) throw new Error(`Missing environment variable: ${env}`);
}

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

// Database connection with pooling and schema verification
async function getDbConnection() {
  const pool = new Pool({
    connectionString: SQL_URI.replace('postgresql+psycopg2', 'postgresql'),
    max: 20,
    min: 5,
    idleTimeoutMillis: 600000,
    connectionTimeoutMillis: 30000,
  });

  pool.on('error', (err) => {
    console.error(`Database pool error: ${err.message || 'Unknown pool error'}, stack: ${err.stack || 'No stack trace'}`);
  });

  try {
    await retry(
      async () => {
        const client = await pool.connect();
        client.release();
      },
      {
        retries: 5,
        factor: 2,
        minTimeout: 1000,
        maxTimeout: 5000,
        onRetry: (err) => console.warn(`Database connection retry: ${err.message || 'Unknown connection error'}`),
      }
    );

    const tables = await pool.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('balances', 'payments')"
    );
    const tableNames = tables.rows.map((row) => row.table_name);
    if (!tableNames.includes('balances') || !tableNames.includes('payments')) {
      throw new Error('Required tables (balances, payments) not found. Ensure database initialization.');
    }

    const columns = await pool.query(
      "SELECT column_name FROM information_schema.columns WHERE table_name = 'payments'"
    );
    const columnNames = columns.rows.map((row) => row.column_name);
    const requiredColumns = ['id', 'address', 'amount', 'tx_id', 'timestamp', 'notified'];
    for (const col of requiredColumns) {
      if (!columnNames.includes(col)) {
        throw new Error(`Payments table missing required column: ${col}`);
      }
    }

    const indexes = await pool.query(
      "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_payments_timestamp'"
    );
    if (!indexes.rows.some((row) => row.indexname === 'idx_payments_timestamp')) {
      console.warn('Missing idx_payments_timestamp index; cleanupOldPayments performance may be suboptimal');
    }

    return pool;
  } catch (err) {
    await pool.end().catch(() => {});
    throw new Error(`Failed to connect to database or verify schema: ${err.message || 'Unknown database error'}, stack: ${err.stack || 'No stack trace'}`);
  }
}

// Fetch and aggregate balances by base address
async function fetchBalances(db) {
  const query = 'SELECT address, available_balance FROM balances WHERE available_balance >= 1000000000';
  try {
    const { rows } = await db.query(query);
    // Aggregate balances by base address
    const balanceMap = new Map();
    for (const row of rows) {
      const baseAddress = stripWorkerSuffix(row.address);
      const balance = BigInt(row.available_balance);
      if (balanceMap.has(baseAddress)) {
        balanceMap.set(baseAddress, {
          address: baseAddress,
          balance: balanceMap.get(baseAddress).balance + balance,
        });
      } else {
        balanceMap.set(baseAddress, {
          address: baseAddress,
          balance,
        });
      }
    }
    const balances = Array.from(balanceMap.values());
    if (balances.length === 0) console.warn('No eligible balances for payout (min_balance = 10 VE)');
    return balances;
  } catch (err) {
    throw new Error(`Failed to fetch balances: ${err.message || 'Unknown query error'}, stack: ${err.stack || 'No stack trace'}`);
  }
}

// Reset balance in database for all entries with the same base address
async function resetBalance(db, address) {
  const baseAddress = stripWorkerSuffix(address);
  try {
    await db.query('UPDATE balances SET available_balance = 0 WHERE address LIKE $1', [`${baseAddress}%`]);
  } catch (err) {
    throw new Error(`Failed to reset balance for ${baseAddress}: ${err.message || 'Unknown update error'}, stack: ${err.stack || 'No stack trace'}`);
  }
}

// Record payment in database
async function recordPayment(db, vecno, address, amount, tx_id, NETWORK_ID) {
  const timestamp = Math.floor(Date.now() / 1000);
  try {
    await db.query(
      'INSERT INTO payments (address, amount, tx_id, timestamp, notified) VALUES ($1, $2, $3, $4, $5)',
      [address, amount.toString(), tx_id, timestamp, false]
    );
    console.log(`Payment recorded: address=${address}, amount=${vecno.sompiToVecnoStringWithSuffix(BigInt(amount), NETWORK_ID)}, tx_id=${tx_id}`);
  } catch (err) {
    throw new Error(`Failed to record payment for ${address}: ${err.message || 'Unknown insert error'}, stack: ${err.stack || 'No stack trace'}`);
  }
}

// Clean up old payments
async function cleanupOldPayments(db, retentionPeriodDays = 30) {
  const cutoffTimestamp = Math.floor(Date.now() / 1000) - retentionPeriodDays * 24 * 60 * 60;
  try {
    const result = await db.query('DELETE FROM payments WHERE timestamp < $1', [cutoffTimestamp]);
    if (result.rowCount > 0) {
      console.log(`Cleaned up ${result.rowCount} old payments older than ${retentionPeriodDays} days`);
    }
  } catch (err) {
    throw new Error(`Failed to clean up old payments: ${err.message || 'Unknown cleanup error'}, stack: ${err.stack || 'No stack trace'}`);
  }
}

// Process payouts with transaction isolation and dynamic fee calculation
async function processPayouts(rpcClient, vecno, createTransactions, db, context, privateKey, MINING_ADDR, NETWORK_ID, processor) {
  let isProcessing = false;
  if (isProcessing) {
    console.warn('Payout processing already in progress; skipping');
    return [];
  }
  isProcessing = true;

  try {
    // Validate inputs
    if (!rpcClient || !vecno || !createTransactions || !db || !context || !privateKey || !MINING_ADDR || !NETWORK_ID || !processor) {
      throw new Error(`Missing required dependencies: ${JSON.stringify({
        rpcClient: !!rpcClient,
        vecno: !!vecno,
        createTransactions: !!createTransactions,
        db: !!db,
        context: !!context,
        privateKey: !!privateKey,
        MINING_ADDR: !!MINING_ADDR,
        NETWORK_ID: !!NETWORK_ID,
        processor: !!processor,
      })}`);
    }

    // Check node sync status
    const serverInfo = await retry(
      async () => {
        const info = await rpcClient.getServerInfo();
        if (!info) throw new Error('Failed to retrieve server info');
        return info;
      },
      {
        retries: 3,
        factor: 2,
        minTimeout: 1000,
        maxTimeout: 5000,
        onRetry: (err) => console.warn(`RPC server info retry: ${err.message || 'Unknown RPC error'}`),
      }
    );
    if (!serverInfo.isSynced) {
      throw new Error('Node not synced; cannot process payouts');
    }

    // Fetch and aggregate balances
    const balances = await fetchBalances(db);
    if (!balances.length) {
      console.warn('No eligible balances for payout (min_balance = 10 VE)');
      return [];
    }

    const payments = balances
      .filter(({ address }) => address !== MINING_ADDR)
      .map(({ address, balance }, index) => ({
        address,
        amount: balance,
        paymentId: index,
      }));

    if (!payments.length) {
      console.warn('No eligible payouts after filtering');
      return [];
    }

    // Fetch UTXOs
    await retry(
      async () => {
        const utxos = (await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] })).entries || [];
        if (!utxos.length) {
          throw new Error(`No UTXOs available for ${MINING_ADDR}`);
        }
      },
      {
        retries: 3,
        factor: 2,
        minTimeout: 1000,
        maxTimeout: 5000,
        onRetry: (err) => console.warn(`UTXO fetch retry: ${err.message || 'Unknown UTXO error'}`),
      }
    );

    const utxos = (await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] })).entries || [];
    if (!utxos.length) {
      console.error(`No UTXOs found for ${MINING_ADDR} after retries`);
      try {
        await processor.stop();
        await processor.start();
        await context.clear();
        await context.trackAddresses([MINING_ADDR]);
        const retryUtxos = (await rpcClient.getUtxosByAddresses({ addresses: [MINING_ADDR] })).entries || [];
        if (!retryUtxos.length) {
          throw new Error('Still no UTXOs available after processor restart');
        }
      } catch (err) {
        throw new Error(`Failed to restart UtxoProcessor: ${err.message || 'Unknown processor error'}, stack: ${err.stack || 'No stack trace'}`);
      }
    }

    console.log(`Available UTXOs: ${utxos.length}, total amount: ${utxos.reduce((sum, u) => sum + BigInt(u.amount), 0n)} sompi`);

    // Calculate total payment amount
    const totalPaymentAmount = payments.reduce((sum, p) => sum + BigInt(p.amount), 0n);
    console.log(`Total payment amount: ${totalPaymentAmount} sompi`);

    // Estimate transaction fee with retries
    let feePerByte = 2000n; // Default fee rate (sompi per byte)
    let feeSource = 'default';
    try {
      const feeEstimate = await retry(
        async () => {
          const estimate = await rpcClient.getFeeEstimate(null);
          console.log('getFeeEstimate response:', serializeBigInt(estimate));
          return estimate;
        },
        {
          retries: 3,
          factor: 2,
          minTimeout: 1000,
          maxTimeout: 5000,
          onRetry: (err) => console.warn(`getFeeEstimate retry: ${err.message || 'Unknown error'}`),
        }
      );
      if (feeEstimate?.estimate?.normalBuckets?.length && BigInt(feeEstimate.estimate.normalBuckets[0].feerate) > 0n) {
        feePerByte = BigInt(feeEstimate.estimate.normalBuckets[0].feerate);
        if (feePerByte < 1000n) {
          console.warn(`Fee rate ${feePerByte} sompi/byte is below minimum threshold; using 1000 sompi/byte`);
          feePerByte = 1000n;
        } else {
          feeSource = 'getFeeEstimate';
          console.log(`Fetched fee rate from getFeeEstimate: ${feePerByte} sompi/byte`);
        }
      } else {
        console.warn('Invalid fee estimate from getFeeEstimate; using default fee rate');
      }
    } catch (err) {
      console.warn(`Failed to fetch fee estimate: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}; using default fee rate`);
    }

    // Create transaction outputs
    const transactionOutputs = payments.map((p) => ({
      address: p.address,
      amount: p.amount,
      paymentId: p.paymentId,
    }));

    // Get maximum standard transaction mass
    const maxMass = vecno.maximumStandardTransactionMass();
    console.log(`Maximum standard transaction mass: ${maxMass}`);

    // Estimate transaction fee
    let estimatedFee = 0n;
    let transactions = [];
    let summary = {};

    try {
      // Split payments into batches to avoid exceeding maxMass
      const batchSize = Math.max(1, Math.floor(100 / payments.length)); // Adjust based on payment count
      const paymentBatches = [];
      for (let i = 0; i < payments.length; i += batchSize) {
        paymentBatches.push(payments.slice(i, i + batchSize));
      }

      let totalFee = 0n;
      const batchTransactions = [];
      const maxAttempts = 3;

      for (const [batchIndex, batchPayments] of paymentBatches.entries()) {
        const batchOutputs = batchPayments.map((p) => ({
          address: p.address,
          amount: p.amount,
          paymentId: p.paymentId,
        }));

        let batchFee = 0n;
        let batchTxs, batchSummary;

        // Calculate fee for batch
        try {
          const result = await retry(
            async () => {
              const res = await createTransactions({
                entries: context,
                outputs: batchOutputs,
                changeAddress: MINING_ADDR,
                priorityFee: 0n, // Temporary for fee estimation
              });
              console.log(`Batch ${batchIndex} temporary createTransactions summary:`, serializeBigInt(res.summary));
              return res;
            },
            {
              retries: maxAttempts,
              factor: 2,
              minTimeout: 1000,
              maxTimeout: 5000,
              onRetry: (err) => console.warn(`Batch ${batchIndex} temporary createTransactions retry: ${err.message || 'Unknown error'}`),
            }
          );
          batchTxs = result.transactions;
          batchSummary = result.summary;

          if (!batchTxs?.length) {
            throw new Error(`No transactions created for batch ${batchIndex}; check UTXO or output configuration`);
          }

          // Calculate fee for each transaction
          for (const tx of batchTxs) {
            try {
              console.log(`Calculating fee for batch ${batchIndex} transaction ${tx.id || 'unknown'}:`, serializeBigInt(tx.transaction));
              const fee = await vecno.calculateTransactionFee(NETWORK_ID, tx.transaction, null);
              if (fee === undefined) {
                throw new Error(`Transaction ${tx.id || 'unknown'} exceeds maximum standard transaction mass (${maxMass})`);
              }
              batchFee += BigInt(fee);
            } catch (feeErr) {
              console.error(`Failed to calculate fee for batch ${batchIndex} transaction ${tx.id || 'unknown'}: ${feeErr.message || 'Unknown error'}, stack: ${feeErr.stack || 'No stack trace'}`);
              throw feeErr;
            }
          }
        } catch (err) {
          throw new Error(`Failed to process batch ${batchIndex}: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
        }

        totalFee += batchFee;
        batchTransactions.push(...batchTxs);
      }

      estimatedFee = totalFee;
      console.log(`Estimated total fee: ${estimatedFee} sompi (using ${feeSource} fee rate: ${feePerByte} sompi/byte)`);

      // Calculate total required amount
      const totalRequired = totalPaymentAmount + estimatedFee;
      const availableBalance = utxos.reduce((sum, utxo) => sum + BigInt(utxo.amount), 0n);
      if (totalRequired > availableBalance) {
        throw new Error(
          `Insufficient UTXO balance: required=${totalRequired}, available=${availableBalance}, payments=${totalPaymentAmount}, estimatedFee=${estimatedFee}`
        );
      }

      // Create final transactions with calculated fee
      try {
        const result = await retry(
          async () => {
            const res = await createTransactions({
              entries: context,
              outputs: transactionOutputs,
              changeAddress: MINING_ADDR,
              priorityFee: estimatedFee,
            });
            console.log('Final createTransactions inputs:', serializeBigInt(res.transactions.map(t => t.transaction.inputs)));
            console.log('Final createTransactions outputs:', serializeBigInt(res.transactions.map(t => t.transaction.outputs)));
            return res;
          },
          {
            retries: maxAttempts,
            factor: 2,
            minTimeout: 1000,
            maxTimeout: 5000,
            onRetry: (err) => console.warn(`Final createTransactions retry: ${err.message || 'Unknown error'}`),
          }
        );
        transactions = result.transactions;
        summary = result.summary;
      } catch (err) {
        throw new Error(`Failed to create final transactions: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
      }
    } catch (err) {
      throw new Error(`Transaction creation failed: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
    }

    if (!transactions?.length) {
      throw new Error('No transactions created; check UTXO or output configuration');
    }

    console.log(`Created ${transactions.length} transactions, summary: ${serializeBigInt(summary)}`);

    const processedPayments = [];
    const processedPaymentIds = new Set();
    const client = await db.connect();

    try {
      await client.query('BEGIN');

      for (const transaction of transactions) {
        if (!transaction?.transaction?.inputs?.length) {
          console.error(`Invalid transaction structure for ID ${transaction.id || 'unknown'}`);
          continue;
        }

        // Log transaction outputs for debugging
        console.log(`Transaction ${transaction.id} outputs:`, serializeBigInt(transaction.transaction.outputs));

        // Sign transaction
        try {
          await transaction.sign([privateKey]);
          if (!transaction.transaction.inputs.every((input) => input.signatureScript?.length)) {
            throw new Error(`Signature script empty for transaction ${transaction.id || 'unknown'}`);
          }
        } catch (signErr) {
          throw new Error(`Failed to sign transaction ${transaction.id || 'unknown'}: ${signErr.message || 'Unknown error'}, stack: ${signErr.stack || 'No stack trace'}`);
        }

        // Submit transaction
        const submissionResponse = await retry(
          async () => {
            const response = await transaction.submit(rpcClient);
            if (!response) throw new Error('Transaction submission returned no response');
            return response;
          },
          {
            retries: 3,
            factor: 2,
            minTimeout: 1000,
            maxTimeout: 5000,
            onRetry: (err) => console.warn(`Transaction submission retry for ${transaction.id || 'unknown'}: ${err.message || 'Unknown submission error'}`),
          }
        );
        console.log(`Transaction ${transaction.id} submitted: ${serializeBigInt(submissionResponse)}`);

        // Record payments
        let matchedOutputs = 0;
        for (const output of transaction.transaction.outputs || []) {
          const outputAmount = BigInt(output.value);
          const matchingPayment = payments.find(
            (p) => Math.abs(Number(p.amount) - Number(outputAmount)) <= 1000 && !processedPaymentIds.has(p.paymentId)
          );

          if (matchingPayment) {
            await recordPayment(client, vecno, matchingPayment.address, matchingPayment.amount, transaction.id, NETWORK_ID);
            await resetBalance(client, matchingPayment.address);
            processedPayments.push({
              address: matchingPayment.address,
              amount: matchingPayment.amount.toString(),
              txId: transaction.id,
            });
            processedPaymentIds.add(matchingPayment.paymentId);
            matchedOutputs++;

          }
        }
        console.log(`Transaction ${transaction.id} matched ${matchedOutputs} outputs`);
      }

      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw new Error(`Payout transaction failed: ${err.message || 'Unknown transaction error'}, stack: ${err.stack || 'No stack trace'}`);
    } finally {
      client.release();
    }

    if (processedPayments.length < payments.length) {
      console.warn(`Processed ${processedPayments.length} out of ${payments.length} payments; some payments may not have matched transaction outputs`);
      console.log('Unmatched payments:', serializeBigInt(payments.filter(p => !processedPaymentIds.has(p.paymentId))));
    }

    return processedPayments;
  } catch (err) {
    console.error(`Payout processing failed: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
    return [];
  } finally {
    isProcessing = false;
  }
}

// Initialize
async function init() {
  let vecno, RpcClient, PrivateKey, Mnemonic, XPrv, UtxoProcessor, UtxoContext, createTransactions;
  let db, rpcClient, processor;
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

    // Initialize RPC with retry
    rpcClient = new RpcClient({ url: RPC_URL, encoding: 'borsh', networkId: NETWORK_ID });
    await retry(
      async () => {
        await rpcClient.connect();
        const serverInfo = await rpcClient.getServerInfo();
        if (!serverInfo?.isSynced) {
          throw new Error('Node not synced');
        }
      },
      {
        retries: 5,
        factor: 2,
        minTimeout: 1000,
        maxTimeout: 5000,
        onRetry: (err) => console.warn(`RPC connection retry: ${err.message || 'Unknown RPC error'}`),
      }
    );

    db = await getDbConnection();
    await cleanupOldPayments(db);

    processor = new UtxoProcessor({ rpc: rpcClient, networkId: NETWORK_ID });
    const context = new UtxoContext({ processor });
    processor.addEventListener('utxo-proc-start', async () => {
      try {
        await context.clear();
        await context.trackAddresses([MINING_ADDR]);
      } catch (err) {
        console.error(`Failed to initialize UtxoContext: ${err.message || 'Unknown context error'}, stack: ${err.stack || 'No stack trace'}`);
      }
    });
    await processor.start();
    await new Promise((resolve) => setTimeout(resolve, 5000));

    // Schedule payouts every 4 minutes
    setInterval(
      async () => {
        try {
          const result = await processPayouts(rpcClient, vecno, createTransactions, db, context, privateKey, MINING_ADDR, NETWORK_ID, processor);
          console.log('Scheduled payout result:', serializeBigInt(result));
        } catch (err) {
          console.error(`Scheduled payout failed: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
        }
      },
      4 * 60 * 1000
    );

    // Initial payout
    const initialResult = await processPayouts(rpcClient, vecno, createTransactions, db, context, privateKey, MINING_ADDR, NETWORK_ID, processor);
    console.log('Initial payout result:', serializeBigInt(initialResult));

    // Cleanup on exit
    const cleanup = async () => {
      try {
        await processor?.stop();
        await rpcClient?.disconnect();
        await db?.end();
        console.log('Shutdown complete');
      } catch (err) {
        console.error(`Shutdown error: ${err.message || 'Unknown shutdown error'}, stack: ${err.stack || 'No stack trace'}`);
      }
      process.exit(0);
    };

    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);

    process.on('uncaughtException', async (err) => {
      console.error(`Uncaught exception: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
      await cleanup();
      process.exit(1);
    });

    process.on('unhandledRejection', async (err) => {
      console.error(`Unhandled rejection: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
      await cleanup();
      process.exit(1);
    });
  } catch (err) {
    console.error(`Initialization failed: ${err.message || 'Unknown error'}, stack: ${err.stack || 'No stack trace'}`);
    await processor?.stop();
    await rpcClient?.disconnect();
    await db?.end();
    process.exit(1);
  }
}

init();