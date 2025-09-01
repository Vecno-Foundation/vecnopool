globalThis.WebSocket = require('websocket').w3cwebsocket;
const vecno = require('../wasm');
const { parseArgs } = require("../wasm/utils");
const {
    RpcClient,
    Address,
    TransactionInput,
    Transaction,
    signTransaction,
    UtxoEntries,
    PrivateKey,
    Mnemonic,
    XPrv,
    NetworkType,
    vecnoToSompi,
    payToAddressScript
} = vecno;

// Helper function to serialize objects with BigInt
function serializeBigInt(obj) {
    const seen = new WeakSet();
    return JSON.stringify(obj, (key, value) => {
        if (typeof value === 'bigint') return value.toString();
        if (typeof value === 'object' && value !== null) {
            if (seen.has(value)) return undefined;
            seen.add(value);
            if (value instanceof Address || value instanceof Transaction || value instanceof TransactionInput ||
                value instanceof UtxoEntries || value instanceof PrivateKey || value instanceof Mnemonic ||
                value instanceof XPrv) {
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

(async () => {
    let rpc;
    try {
        // Parse command-line arguments
        const { encoding = 'borsh', networkId = NetworkType.Mainnet } = parseArgs();
        const rpcUrl = process.env.WRPC_URL || "127.0.0.1:8110";

        // Initialize RPC client
        rpc = new RpcClient({ url: rpcUrl, encoding, networkId });
        console.log(`Connecting to ${rpcUrl}`);
        await rpc.connect();

        // Verify node sync status
        const { isSynced } = await rpc.getServerInfo();
        if (!isSynced) {
            throw new Error('Node is not synced. Please wait for the node to sync.');
        }

        console.log("Server info:", serializeBigInt(await rpc.getInfo()));

        // Derive addresses from mnemonic
        const mnemonicPhrase = "acoustic once under insane delay void exhaust fold click cup raw evolve love pottery alpha often put marble bullet rapid cupboard chair cover supply";
        const paths = ["m/44'/111111'/0'/0/0", "m/44'/111111'/0'/0/1", "m/44'/111111'/0'/1/0"];
        const addresses = [];
        const privateKeys = {};

        console.log("Creating Mnemonic...");
        const mnemonic = new Mnemonic(mnemonicPhrase);
        console.log("Mnemonic created:", mnemonic.phrase);
        const seed = mnemonic.toSeed();
        console.log("Mnemonic seed:", seed.toString('hex'));
        console.log("Mnemonic seed length:", seed.length, "bytes");

        console.log("Creating XPrv...");
        const xPrv = new XPrv(seed);
        console.log("Deriving private keys...");
        for (const path of paths) {
            const key = xPrv.derivePath(path).toPrivateKey();
            const pubKey = key.toPublicKey();
            const address = pubKey.toAddress(NetworkType.Mainnet);
            console.log(`Private key [${path}]:`, key.toString());
            console.log(`Public key [${path}]:`, pubKey.toString());
            console.log(`Address [${path}]:`, address.toString());
            addresses.push(address.toString());
            privateKeys[address.toString()] = { key, pubKey };
        }

        // Fetch UTXOs for addresses
        console.log("\nFetching UTXOs for addresses:", addresses);
        const utxos = await rpc.getUtxosByAddresses({ addresses });
        if (!utxos.entries || utxos.entries.length === 0) {
            throw new Error(`No UTXOs found for addresses: ${addresses.join(', ')}`);
        }

        console.log("UTXOs:", serializeBigInt(utxos.entries));

        // Calculate balances
        const balances = {};
        addresses.forEach(addr => balances[addr] = 0n);
        utxos.entries.forEach(utxo => {
            const addr = utxo.address.toString();
            // Verify P2PK script (68 bytes, starts with '20', ends with 'ac')
            if (utxo.scriptPublicKey.script.length !== 68 || 
                !utxo.scriptPublicKey.script.startsWith('20') || 
                !utxo.scriptPublicKey.script.endsWith('ac')) {
                console.warn(`Skipping non-P2PK UTXO for address ${addr}`);
                return;
            }
            balances[addr] += BigInt(utxo.amount);
        });

        console.log("Balances:");
        addresses.forEach(addr => {
            console.log(`${addr}: ${Number(balances[addr]) / 1e8} VE`);
        });

        // Select address with sufficient balance
        const amount = vecnoToSompi('10'); // 10 VE
        const minimumFee = 210000n; // 0.0021 VE
        const requiredAmount = amount + minimumFee;
        let selectedAddress = null;
        let selectedPrivateKey = null;
        let selectedUtxos = [];

        for (const addr of addresses) {
            if (balances[addr] >= requiredAmount) {
                selectedAddress = addr;
                selectedPrivateKey = privateKeys[addr].key;
                selectedUtxos = utxos.entries.filter(utxo => 
                    utxo.address.toString() === addr &&
                    utxo.scriptPublicKey.script.length === 68 &&
                    utxo.scriptPublicKey.script.startsWith('20') &&
                    utxo.scriptPublicKey.script.endsWith('ac')
                );
                break;
            }
        }

        if (!selectedAddress) {
            throw new Error(`No address has sufficient balance for transaction: ${requiredAmount} sompi`);
        }

        console.log("Selected address:", selectedAddress);
        console.log("Selected UTXOs:", serializeBigInt(selectedUtxos));

        // Define destination and change addresses
        const destinationAddress = "vecno:qz2ut20ajguxycqxdqyset3hvvjz50cszh88u5zy7avyruwxehqdzfefuva4t";
        const changeAddress = selectedAddress;

        // Create transaction inputs
        const utxoEntryList = [];
        const inputs = selectedUtxos.map((utxo, sequence) => {
            utxoEntryList.push(utxo);
            return new TransactionInput({
                previousOutpoint: utxo.outpoint,
                signatureScript: [],
                sequence,
                sigOpCount: 1, // P2PK requires 1 signature operation
                utxo
            });
        });

        const utxoEntries = new UtxoEntries(utxoEntryList);
        console.log("Inputs:", serializeBigInt(inputs));

        // Calculate total input amount
        const totalInputAmount = utxoEntryList.reduce((sum, utxo) => sum + BigInt(utxo.amount), 0n);
        console.log("Total input amount:", totalInputAmount.toString());

        // Create transaction outputs
        const destinationScriptPublicKey = payToAddressScript(destinationAddress);
        const outputs = [
            {
                value: amount,
                scriptPublicKey: destinationScriptPublicKey
            }
        ];
        const changeAmount = totalInputAmount - amount - minimumFee;
        if (changeAmount > 0) {
            outputs.push({
                value: changeAmount,
                scriptPublicKey: utxoEntryList[0].scriptPublicKey // Reuse P2PK script
            });
        } else if (changeAmount < 0) {
            throw new Error("Insufficient funds: total input amount is less than amount + fee");
        }
        console.log("Outputs:", serializeBigInt(outputs));
        console.log("UTXO entries:", serializeBigInt(utxoEntries.items));

        // Create transaction
        const transaction = new Transaction({
            inputs,
            outputs,
            lockTime: 0,
            subnetworkId: new Array(20).fill(0), // Zeroed subnetwork ID
            version: 0,
            gas: 0,
            payload: [],
        });
        console.log("Transaction:", serializeBigInt(transaction));
        console.log("Minimum fee:", minimumFee.toString());

        // Estimate transaction mass and fee
        const storageMassParameter = 2.036e9;
        const computeMass = transactionSerializedByteSize(transaction) + (66 * inputs.length); // 66 bytes per Schnorr signature
        const outputHarmonic = outputs.reduce((sum, out) => sum + (storageMassParameter / Number(out.value)), 0);
        const inputArithmetic = Number(totalInputAmount) / inputs.length;
        const storageMass = outputHarmonic > inputArithmetic ? outputHarmonic - inputArithmetic : 0;
        const totalMass = Math.max(computeMass, storageMass);
        const estimatedFee = Math.ceil((totalMass * 1000) / 1000) || 1000;
        console.log("Estimated compute mass:", computeMass);
        console.log("Estimated storage mass:", storageMass);
        console.log("Total estimated mass:", totalMass);
        console.log("Estimated minimum fee:", estimatedFee);

        // Verify private key matches address
        const publicKey = selectedPrivateKey.toPublicKey();
        const derivedAddress = publicKey.toAddress(NetworkType.Mainnet);
        console.log("Public key:", publicKey.toString());
        console.log("Derived address:", derivedAddress.toString());
        if (derivedAddress.toString() !== selectedAddress) {
            throw new Error("Private key does not match the selected address!");
        }

        // Verify public key matches UTXO scriptPublicKey
        const utxoPubKey = utxoEntryList[0].scriptPublicKey.script.slice(2, 66);
        const derivedPubKey = publicKey.toString().slice(2);
        console.log("UTXO Public Key:", utxoPubKey);
        console.log("Derived Public Key (no prefix):", derivedPubKey);
        if (utxoPubKey !== derivedPubKey) {
            throw new Error("Public key does not match UTXO scriptPublicKey!");
        }

        // Sign transaction with Schnorr
        console.log("Signing transaction with Schnorr...");
        const signedTransaction = signTransaction(transaction, [selectedPrivateKey], true);
        console.log("Signed transaction inputs:", signedTransaction.inputs.map(input => input.signatureScript.toString('hex')));
        console.log("Signature script length:", signedTransaction.inputs[0].signatureScript.length, "bytes");
        console.log("Signature script bytes:", Array.from(signedTransaction.inputs[0].signatureScript).map(b => b.toString(16).padStart(2, '0')).join(''));

        // Verify signature script
        if (signedTransaction.inputs.some(input => !input.signatureScript || input.signatureScript.length === 0)) {
            throw new Error("Signature script is empty after signing!");
        }

        // Submit transaction
        const result = await rpc.submitTransaction({ transaction: signedTransaction, allowOrphan: false });
        console.log("Result:", serializeBigInt(result));
        console.log("Signed transaction:", serializeBigInt(signedTransaction));

        await rpc.disconnect();
    } catch (error) {
        console.error("Error:", error.message);
        if (rpc) await rpc.disconnect();
    }
})();