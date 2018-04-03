let Mvsd = require('./models/Mvsd.js'),
    Messenger = require('./models/Messenger'),
    MongoDB = require('./models/Mongo.js');

//Setup logging
let winston = require('winston'),
    log_config = require('./config/logging.js');
if (log_config.logstash.enabled) {
    winston.info('enable logging', log_config.logstash);
    require('winston-logstash');
    winston.add(winston.transports.Logstash, {
        port: log_config.logstash.port,
        node_name: log_config.logstash.node_name,
        host: log_config.logstash.host
    });
}

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
                console.error(error);
                winston.error('sync block error', {
                    topic: "block",
                    message: error.message,
                    details: error,
                    height: start
                });
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
                            winston.info('block exists', {
                                topic: "block",
                                message: "exists",
                                height: number,
                                hash: header.hash
                            });
                            return null;
                        } else {
                            return Promise.all(block.txs.transactions.map((tx) => {
                                    header.txs.push(tx.hash);
                                    tx.height = number;
                                    tx.orphan = 0;
                                    tx.block = header.hash;
                                    tx.confirmed_at = header.time_stamp;
                                    return organizeTx(tx)
                                        .then((updatedTx) => MongoDB.addTx(updatedTx))
                                        .catch((e) => {
                                            winston.error('add transaction', {
                                                topic: "transaction",
                                                message: e.message,
                                                height: tx.height,
                                                hash: tx.hash,
                                                block: tx.block
                                            });
                                            console.error(e);
                                        });
                                }))
                                .then(() => MongoDB.addBlock(header))
                                .then(() => {
                                    if(number%100000==0)
                                        Messenger.send('Sync milestone', `Block #${number} reached`);
                                    winston.info('block added', {
                                        topic: "block",
                                        message: "added",
                                        height: number,
                                        hash: header.hash
                                    });
                                });
                        }
                    }));
        });
}

function organizeTxOutputs(tx, outputs) {
    return Promise.all(outputs.map((output) => {
        if (output.attachment.type == "etp" || output.attachment.type == "message") {
            output.attachment.symbol = "ETP";
            output.attachment.decimals = 8;
            return output;
        } else if (output.attachment.type == "asset-issue") {
            //delete output.attachment.type;
            output.attachment.decimals = output.attachment.decimal_number;
            delete output.attachment.decimal_number;
            output.attachment.hash = tx.hash;
            output.attachment.height = tx.height;
            newAsset(output.attachment);
            return output;
        } else if (output.attachment.type == "asset-transfer") {
            return MongoDB.getAsset(output.attachment.symbol)
                .then((asset) => {
                    output.attachment.decimals = asset.decimals;
                    return output;
                });
        } else {
            //not handled type of TX
            Messenger.send('Unknow type', `Unknow output type in block ${tx.height}, transaction ${tx.hash}, index ${output.index}`);
            console.log('Unknown output type in blocks %i, transaction %i, index %i', tx.height, tx.hash, output.index);
            winston.error('unknow type', {
                topic: "transaction",
                message: "unknown output type",
                height: tx.height,
                hash: tx.hash,
                block: tx.block,
                index: output.index
            });
            return output;
        }
    }));
}

function organizeTxPreviousOutputs(input) {
    return MongoDB.getTx(input.previous_output.hash)
        .then((previousTx)=>{
            if(previousTx)
                return previousTx;
            else{
                winston.info('transaction load', {
                    topic: "transaction",
                    message: "alternative load from mvsd",
                    hash: input.previous_output.hash
                });
                return Mvsd.getTx(input.previous_output.hash, true);
            }
        })
        .then((previousTx) => {

            var previousOutput = previousTx.outputs[input.previous_output.index];
            input.attachment = {};
            input.value = previousOutput.value;
            input.address = previousOutput.address;
            if (previousOutput.attachment.type == "etp" || output.attachment.type == "message") {
                input.attachment.symbol = "ETP";
                input.attachment.decimals = 8;
                return input;
            } else if (previousOutput.attachment.type == "asset-issue") {
                input.attachment.quantity = previousOutput.attachment.quantity;
                input.attachment.symbol = previousOutput.attachment.symbol;
                input.attachment.decimals = previousOutput.attachment.decimals;
                return input;
            } else if (previousOutput.attachment.type == "asset-transfer") {
                return MongoDB.getAsset(previousOutput.attachment.symbol)
                    .then((asset) => {
                        input.attachment.quantity = previousOutput.attachment.quantity;
                        input.attachment.symbol = previousOutput.attachment.symbol;
                        input.attachment.decimals = asset.decimals;
                        return input;
                    });
            } else {
                //not handled type of TX
                Messenger.send('Unknow type', `Unknow output type in block ${previousTx.height}, transaction ${previousTx.hash}, index ${input.previous_output.index}`);
                console.log('Unknown output type in blocks %i, transaction %i, index %i', previousTx.hash, previousTx.height, input.previous_output.index);
                winston.error('unknow type', {
                    topic: "transaction",
                    message: "unknown output type",
                    height: previousTx.height,
                    hash: previousTx.hash,
                    block: previousTx.block,
                    index: input.previous_output.index
                });
                return input;
            }
        });
}

function organizeTxInputs(inputs) {
    return Promise.all(inputs.map((input) => {
        if (input.previous_output.index < 4294967295) {
            return organizeTxPreviousOutputs(input);
        } else {
            input.attachment = {};
            input.attachment.symbol = "ETP";
            input.attachment.decimals = 8;
            input.address = "";
            input.value = 0;
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
                    winston.warning('fork detected', {
                        topic: "fork",
                        message: "blockchain forked",
                        height: block.number,
                        block: block.hash
                    });
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
            winston.warning('fork resolved', {
                topic: "fork",
                message: "blockchain fork resolved",
                height: number,
                block: forkhead,
                size: forksize
            });
            return forksize;
        });
}

function wait(ms) {
    return new Promise(resolve => setTimeout(() => resolve(), ms));
}

MongoDB.init()
    .then(() => MongoDB.getLastBlock())
    .then((lastblock) => {
        if (lastblock) {
            Messenger.send('Sync start','sync starting from block '+lastblock.number);
            winston.info('sync starting', {
                topic: "sync",
                message: "continue",
                height: lastblock.number
            });
        } else {
            winston.info('sync starting', {
                topic: "sync",
                message: "starting",
                height: 0
            });
        }
        return MongoDB.removeBlock((lastblock) ? lastblock.hash : 0)
            .then(() => syncBlocksFrom((lastblock) ? lastblock.number : 0));
    })
    .catch((error) => {
        console.error(error);
        Messenger.send('Sync exit',error.message);
        winston.error('sync error', {
            topic: "sync",
            message: error.message,
            details: error
        });
        return MongoDB.disconnect();
    });
