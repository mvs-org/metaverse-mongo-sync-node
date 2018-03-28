let Mvsd = require('./models/Mvsd.js'),
    Messenger = require('./models/Messenger'),
    MongoDB = require('./models/Mongo.js');

async function syncBlocksFrom(start) {
    while (true) {
        try {
            await syncBlock(start);
            start++;
        } catch (error) {
            if (error.message == 5101) {
                console.info('nothing to do. retry');
                await wait(5000);
            } else {
                console.error(error)
                throw Error(error.message);
            }
        }
    }
}

function syncBlock(number) {
    return Mvsd.getBlock(number)
        .then((block) => {
            let header = block.header.result;
            header.orphan = 0;
            return detectFork(number - 1, header.previous_block_hash, null, false)
                .then((length) => (length) ? syncBlock(number) :
                    MongoDB.getBlock(header.hash)
                    .then((b) => {
                        header.txs = [];
                        if (b != null) {
                            console.log('block #%i %s exists', number, header.hash);
                            return null;
                        } else {
                            return Promise.all(block.txs.transactions.map((tx) => {
                                    header.txs.push(tx.hash);
                                    tx.height = number;
                                    tx.orphan = 0;
                                    tx.block = header.hash;
                                    return organizeTx(tx)
                                        .then((updatedTx) => MongoDB.addTx(updatedTx))
                                    .catch((e) => {console.error(e)});
                                }))
                                .then(() => MongoDB.addBlock(header))
                                .then(() => console.info('added block #%i %s', number, header.hash));
                        }
                    }));
        });
}

function organizeTxOutputs(tx, outputs) {
    return Promise.all(outputs.map((output) => {
        if (output.attachment.type == "etp") {
            output.assets = "ETP";
            output.decimals = 8;
            return output;
        } else if (output.attachment.type == "asset-issue") {
            output.assets = output.attachment.symbol.toUpperCase();
            delete output.attachment.type;
            output.attachment.hash = tx.hash;
            output.attachment.height = tx.height;
            newAsset(output.attachment);
            return output;
        } else {
            return MongoDB.getAsset(output.attachment.symbol)
                .then((asset) => {
                    output.assets = output.attachment.symbol.toUpperCase();
                    output.decimals = asset.decimal_number;
                    return output;
                });
        }
    }));
}

function organizeTxPreviousOutputs(input) {
    return Mvsd.getTx(input.previous_output.hash, true)
        .then((previousTx) => {
            var previousOutput = previousTx.outputs[input.previous_output.index];
            if (previousOutput.attachment.type == "etp") {
                input.assets = "ETP";
                input.decimals = 8;
                input.value = previousOutput.value;
                return input;
            } else if (previousOutput.attachment.type == "asset-issue") {
                input.value = previousOutput.attachment.quantity;
                input.asset = previousOutput.attachment.symbol.toUpperCase();
                input.decimals = previousOutput.attachment.decimal_number;
                return input;
            } else {
                return MongoDB.getAsset(previousOutput.attachment.symbol)
                    .then((asset) => {
                        input.value = previousOutput.attachment.quantity;
                        input.asset = previousOutput.attachment.symbol.toUpperCase();
                        input.decimals = asset.decimal_number;
                        return input;
                    });
            }
        });
}

function organizeTxInputs(inputs) {
    return Promise.all(inputs.map((input) => {
        if (input.previous_output.index < 4294967295) {
            return organizeTxPreviousOutputs(input);
        } else {
            //Coinbase input -> nothing to organize
            return input;
        }
    }));
}

function organizeTx(tx) {
    return Promise.all([
            organizeTxOutputs(tx, tx.outputs),
            organizeTxInputs(tx.inputs),
            Mvsd.getTx(tx.hash, false).then((res) => res.transaction.raw)
        ])
        .then((results) => {
            tx.outputs = results[0];
            tx.inputs = results[1];
            tx.rawtx = results[2];
            return tx;
        });
}

function newAsset(output) {
    return MongoDB.addAsset(output);
}

function detectFork(number, hash, forkhead, is_fork) {
    return MongoDB.getBlockByNumber(number)
        .then((block) => {
            if (block && block.hash != hash) {
                if (!is_fork) {
                    Messenger.send('Fork Detected', `Detected fork on block ${block.hash}`);
                    console.log('fork detected!!!');
                }
                return MongoDB.getBlock(hash)
                    .then(() => detectFork(number - 1, block.previous_block_hash, (forkhead) ? forkhead : block.hash, true));
            } else {
                if (!is_fork)
                    return null;
                else
                    return applyFork(number + 1, forkhead);
            }
        });
}

function applyFork(number, forkhead) {
    return MongoDB.markOrphanFrom(number, forkhead)
        .then((forksize) => {
            Messenger.send('Fork Resolved', `Forked ${forksize} blocks from ${number} to block ${forkhead}`);
            console.log('forked %i blocks from %i to block %s', forksize, number, forkhead);
            return forksize;
        });
}

function wait(ms) {
    return new Promise(resolve => setTimeout(() => resolve(), ms));
}

MongoDB.init()
    .then(() => MongoDB.getLastBlock())
    .then((lastblock) => {
        if (lastblock)
            Messenger.send('Sync Starting', `Starting to sync from ${lastblock.number}`);
        else
            Messenger.send('Sync Starting', `Starting to sync from 0`);
        return MongoDB.removeBlock((lastblock) ? lastblock.hash : 0)
            .then(() => syncBlocksFrom((lastblock) ? lastblock.number : 0));
    })
    .catch((error) => {
        console.error(error);
        Messenger.send('Sync Error', error.message);
        return MongoDB.disconnect();
    });
