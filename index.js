let Mvsd = require('./models/Mvsd.js');
let MongoDB = require('./models/Mongo.js');

function syncBlocksFrom(start) {
    return syncBlock(start)
        .then(() => syncBlocksFrom(++start))
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
                .then((length) => {
                    if (length)
                        return syncBlock(number);
                    else {
                        return MongoDB.getBlock(header.hash)
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
                                        .then(() => Promise.all(block.txs.transactions.map((tx) => {
                                            return MongoDB.markOutputsAsSpent(tx);
                                        })))
                                        .then(() => MongoDB.addBlock(header))
                                        .then(() => console.info('added block #%i %s', number, header.hash));
                                }
                            });
                    }
                });
        });
}

function detectFork(number, hash, forkhead, is_fork) {
    return MongoDB.getBlockByNumber(number)
        .then((block) => {
            if (block && block.hash != hash) {
                if (!is_fork) {
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
            console.log('forked %i blocks from %i to block %s', forksize, number, forkhead);
            return forksize;
        });
}

MongoDB.init()
    .then(() => MongoDB.getLastBlock())
    .then((lastblock) => {
        return MongoDB.removeBlock((lastblock) ? lastblock.hash : 0)
            .then(() => syncBlocksFrom((lastblock) ? lastblock.number : 0));
    })
    .catch((error) => {
        console.error(error);
        return MongoDB.disconnect();
    });
