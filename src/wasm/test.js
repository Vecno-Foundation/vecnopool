globalThis.WebSocket = require('websocket').w3cwebsocket;
const vecno = require('../wasm');
const { parseArgs } = require("../wasm/utils");
const {
    RpcClient,
    Address,
    PrivateKey,
    Mnemonic,
    XPrv,
    NetworkType,
    sompiToVecnoStringWithSuffix,
    UtxoProcessor,
    UtxoContext,
    createTransactions
} = vecno;

// Helper function to serialize objects with BigInt
function serializeBigInt(obj) {
    const seen = new WeakSet();
    return JSON.stringify(obj, (key, value) => {
        if (typeof value === 'bigint') return value.toString();
        if (typeof value === 'object' && value !== null) {
            if (seen.has(value)) return undefined;
            seen.add(value);
            if (value instanceof Address || value instanceof vecno.Transaction || value instanceof vecno.TransactionInput ||
                value instanceof vecno.UtxoEntries || value instanceof PrivateKey || value instanceof Mnemonic ||
                value instanceof XPrv) {
                return Object.fromEntries(
                    Object.entries(value).filter(([k, v]) => typeof v !== 'function' && !(v instanceof Buffer))
                );
            }
        }
        return value;
    }, 2);
}

(async () => {
    let rpc, processor, context;
    try {
        // Parse command-line arguments
        const { encoding = 'borsh', networkId = NetworkType.Mainnet } = parseArgs();
        const rpcUrl = process.env.WRPC_URL || "ws://127.0.0.1:8110";

        // Initialize RPC client
        console.log(`Connecting to ${rpcUrl}`);
        rpc = new RpcClient({ url: rpcUrl, encoding, networkId });
        await rpc.connect();
        console.log("RPC connection established");

        // Verify node sync status
        const { isSynced } = await rpc.getServerInfo();
        if (!isSynced) {
            throw new Error('Node is not synced. Please wait for the node to sync.');
        }
        console.log("Server info:", serializeBigInt(await rpc.getInfo()));

        // Derive address from mnemonic
        const mnemonicPhrase = "acoustic once under insane delay void exhaust fold click cup raw evolve love pottery alpha often put marble bullet rapid cupboard chair cover supply";
        const path = "m/44'/111111'/0'/0/0";
        console.log("Creating Mnemonic...");
        const mnemonic = new Mnemonic(mnemonicPhrase);
        console.log("Mnemonic created:", mnemonic.phrase);
        const seed = mnemonic.toSeed();
        console.log("Mnemonic seed:", seed.toString('hex'));
        console.log("Mnemonic seed length:", seed.length, "bytes");

        console.log("Creating XPrv...");
        const xPrv = new XPrv(seed);
        console.log("Deriving private key...");
        const privateKey = xPrv.derivePath(path).toPrivateKey();
        const pubKey = privateKey.toPublicKey();
        const address = pubKey.toAddress(NetworkType.Mainnet);
        console.log(`Private key [${path}]:`, privateKey.toString());
        console.log(`Public key [${path}]:`, pubKey.toString());
        console.log(`Address [${path}]:`, address.toString());

        // Initialize UtxoProcessor and UtxoContext
        processor = new UtxoProcessor({ rpc, networkId });
        context = new UtxoContext({ processor });
        processor.addEventListener("utxo-proc-start", async () => {
            console.log("Clearing UtxoContext and tracking address...");
            await context.clear();
            await context.trackAddresses([address.toString()]);
            console.log("UtxoContext tracking initialized");
        });
        console.log("Starting UtxoProcessor...");
        await processor.start();
        console.log("UtxoProcessor started");

        // Wait for UTXO processor to initialize
        console.log("Waiting for UTXO processor to initialize...");
        await new Promise(resolve => setTimeout(resolve, 5000)); // 5-second timeout

        // Define test payments
        const payments = [
            {
                address: "vecno:qz2ut20ajguxycqxdqyset3hvvjz50cszh88u5zy7avyruwxehqdzfefuva4t",
                amount: BigInt(10_000_000_000) // 10 VE in sompi
            },
            {
                address: "vecno:qrhqvg5m9txyvtw52jweww5xv05mnpqp8u5cp5tgcj63fnydsp6xkjgqfadrj",
                amount: BigInt(5_000_000_000) // 5 VE in sompi
            }
        ];

        console.log("Payments to process:", payments.map(p => ({
            address: p.address,
            amount: sompiToVecnoStringWithSuffix(p.amount, networkId)
        })));

        if (payments.length === 0) {
            throw new Error("No payments to process");
        }

        // Create and submit batched transactions
        console.log("Creating transactions...");
        let transactions, summary;
        try {
            ({ transactions, summary } = await createTransactions({
                entries: context,
                outputs: payments,
                changeAddress: address.toString(),
                priorityFee: 0n // Let createTransactions estimate fee
            }));
        } catch (txError) {
            throw new Error(`Failed to create transactions: ${txError.message}`);
        }

        console.log("Transaction summary:", serializeBigInt(summary));
        console.log(`Number of transactions: ${transactions.length}`);
        console.log("Transactions:", serializeBigInt(transactions));

        if (transactions.length === 0) {
            throw new Error("No transactions created. Ensure the address has sufficient UTXOs.");
        }

        for (const transaction of transactions) {
            console.log(`Processing transaction ${transaction.id}...`);
            // Check transaction structure
            if (!transaction.transaction || !Array.isArray(transaction.transaction.inputs)) {
                throw new Error(`Invalid transaction structure for ID ${transaction.id}: missing transaction or inputs`);
            }
            console.log(`Transaction inputs for ${transaction.id}:`, serializeBigInt(transaction.transaction.inputs));
            console.log(`Signing transaction ${transaction.id}...`);
            await transaction.sign([privateKey]);
            // Verify signatures
            if (!transaction.transaction.inputs.every(input => input.signatureScript && input.signatureScript.length > 0)) {
                throw new Error(`Signature script is empty or missing for some inputs in transaction ${transaction.id}`);
            }
            console.log(`Submitting transaction ${transaction.id}...`);
            const submissionResponse = await transaction.submit(rpc);
            console.log(`Transaction ${transaction.id} submitted successfully. Response:`, serializeBigInt(submissionResponse));
            await new Promise(resolve => setTimeout(resolve, 10000)); // 10-second delay to allow node processing
        }

        // Log final transaction details
        console.log("Raw summary object:", serializeBigInt(summary));
        try {
            if (!summary || typeof summary !== 'object') {
                throw new Error("Summary object is invalid or undefined");
            }
            console.log("Final transaction ID:", summary.finalTransactionId || "undefined");
            console.log("Total amount:", summary.finalAmount != null && typeof summary.finalAmount === 'bigint'
                ? sompiToVecnoStringWithSuffix(summary.finalAmount, networkId)
                : "undefined");
            console.log("Total fee:", summary.fees != null && typeof summary.fees === 'bigint'
                ? sompiToVecnoStringWithSuffix(summary.fees, networkId)
                : "undefined");
        } catch (summaryError) {
            console.error("Error logging transaction summary:", summaryError.message);
            console.error("Summary error stack:", summaryError.stack);
        }

        // Wait before cleanup to ensure node processing
        console.log("Waiting before cleanup...");
        await new Promise(resolve => setTimeout(resolve, 5000));

        // Clean up
        console.log("Cleaning up...");
        try {
            console.log("Stopping UtxoProcessor...");
            await processor.stop();
            console.log("UtxoProcessor stopped");
        } catch (stopError) {
            console.error("Error stopping UtxoProcessor:", stopError.message);
            console.error("UtxoProcessor error stack:", stopError.stack);
        }
        try {
            console.log("Disconnecting RPC...");
            await rpc.disconnect();
            console.log("RPC disconnected");
        } catch (disconnectError) {
            console.error("Error disconnecting RPC:", disconnectError.message);
            console.error("RPC disconnect error stack:", disconnectError.stack);
        }
        console.log("Cleanup complete");
    } catch (error) {
        console.error("Error:", error.message);
        console.error("Error stack:", error.stack);
        // Clean up on error
        try {
            console.log("Stopping UtxoProcessor due to error...");
            await processor.stop();
            console.log("UtxoProcessor stopped due to error");
        } catch (stopError) {
            console.error("Error stopping UtxoProcessor during cleanup:", stopError.message);
            console.error("UtxoProcessor cleanup error stack:", stopError.stack);
        }
        try {
            console.log("Disconnecting RPC due to error...");
            await rpc.disconnect();
            console.log("RPC disconnected due to error");
        } catch (disconnectError) {
            console.error("Error disconnecting RPC during cleanup:", disconnectError.message);
            console.error("RPC disconnect cleanup error stack:", disconnectError.stack);
        }
        throw error; // Re-throw to ensure the error is visible
    }
})();