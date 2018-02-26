let Mvsd = require('./models/Mvsd.js'),
    Messenger = require('./models/Messenger'),
    MongoDB = require('./models/Mongo.js');

async function syncBlocksFrom(start) {
    return syncBlock(start)
        .then(async () => {
            return await syncBlocksFrom(++start);
        })
        .catch((error) => {
            if (error.message == 5101) {
                console.info('nothing to do. retry');
                setTimeout(() => syncBlocksFrom(start), 5000);
            } else {
                throw Error(error.message);
            }
        });
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
                        if (b != null) {
                            console.log('block #%i %s exists', number, header.hash);
                            return null;
                        } else {
                            return Promise.all(block.txs.transactions.map((tx) => {
                                    tx.height = number;
                                    tx.block = header.hash;
                                    return MongoDB.addTx(tx).catch(() => {});
                                }))
                                .then(() => Promise.all(block.txs.transactions.map((tx) => MongoDB.markOutputsAsSpent(tx))))
                                .then(() => Promise.all(block.txs.transactions.map((tx) => MongoDB.includeInputsAddresses(tx))))
                                .then(() => MongoDB.addBlock(header))
                                .then(() => console.info('added block #%i %s', number, header.hash));
                        }
                    }));
        });
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

MongoDB.init()
    .then(() => MongoDB.getLastBlock())
    .then((lastblock) => {
        Messenger.send('Sync Starting', `Starting to sync from ${lastblock.number}`);
        return MongoDB.removeBlock((lastblock) ? lastblock.hash : 0)
            .then(() => syncBlocksFrom((lastblock) ? lastblock.number : 0));
    })
    .catch((error) => {
        console.error(error);
        Messenger.send('Sync Error', error.message);
        return MongoDB.disconnect();
    });
