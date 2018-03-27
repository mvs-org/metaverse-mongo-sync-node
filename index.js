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
                                    tx.outputs.forEach(function(output) {
                                        if(output.attachment.type == "asset-issue"){
                                            delete output.attachment.type
                                            output.attachment.hash = tx.hash
                                            output.attachment.height = tx.height
                                            newAsset(output.attachment)
                                        }
                                    });
                                    return organizeTx(tx)
                                        .then((updatedTx) => MongoDB.addTx(updatedTx))
                                        .catch(() => {});
                                }))
                                .then(() => MongoDB.addBlock(header))
                                .then(() => console.info('added block #%i %s', number, header.hash));
                        }
                    }));
        });
}

function organizeTx(tx) {
    tx.outputs.forEach(function(output) {
        if (output.attachment.type == 'etp') {
          output.attachment.decimal = 8
        } else {
          MongoDB.getAsset(output.attachment.type)
              .then((asset) => {
                  output.attachment.decimal = asset.decimal_number
              })
        }})
    tx.inputs.forEach(function(input) {
        if(input.previous_output.index < 4294967295) {
            Mvsd.getTx(input.previous_output.hash, true)
                .then((previousTx) => {
                    previousTx.output.forEach(function(previousOutput) {
                        if(previousOutput.index == input.previous_output.index)
                            input.value = previousOutput.value
                    });
            })
        }
    })
    return MongoDB.getTx(tx.hash)
        .then((raw_transaction) => {
            tx.rawtx = raw_transaction.transaction.raw;
        })
        .then(() => {
            return tx;
        })
}

function newAsset(output) {
    MongoDB.addAsset(output)
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
        if(lastblock)
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
