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
            let orphaned = await syncBlock(start);
            if (orphaned)
                start -= orphaned;
            else
                start++;
            if (start >= 1000 && start % 100 == 0)
                await MongoDB.prepareStats(start - 100);
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
                await wait(2000);
                throw Error(error.message);
            }
        }
    }
}

function syncBlock(number) {
    let inputs = [];
    return Mvsd.getBlock(number)
        .then((block) => {
            let header = block.header.result;
            header.orphan = 0;
            return detectFork(number - 1, header.previous_block_hash, null, false)
                .then((length) => (length) ? length :
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
                                    return Promise.all(((tx.outputs) ? tx.outputs : []).map((output) => {
                                            output.tx = tx.hash;
                                            output.orphaned_at = 0;
                                            output.height = tx.height;
                                            output.spent_tx = 0;
                                            output.confirmed_at = tx.confirmed_at;
                                            return output;
                                        }))
                                        .then((outputs) => MongoDB.addOutputs(outputs)
                                            .catch((e) => {
                                                winston.error('add outputs', {
                                                    topic: "output",
                                                    message: e.message,
                                                    height: tx.height,
                                                    hash: tx.hash,
                                                    block: tx.block
                                                });
                                                console.error(e);
                                                return;
                                            }))
                                        .then(() => Promise.all(tx.inputs.map((input, index) => {
                                            input.tx = tx.hash;
                                            input.index = index;
                                            inputs.push(input);
                                            return input;
                                        })))
                                        .then(() => organizeTx(tx))
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
                                            // throw Error(e.message);
                                        });
                                }))
                                .then(() => MongoDB.addBlock(header))
                                .then(() => Promise.all(inputs.map((input) => {
                                    if (input.previous_output.hash !== "0000000000000000000000000000000000000000000000000000000000000000")
                                        return MongoDB.markSpentOutput(input.tx, input.index, header.number, input.previous_output.hash, input.previous_output.index)
                                            .then((result) => {
                                                if (result)
                                                    winston.info('output spent', {
                                                        topic: "output",
                                                        message: "spent",
                                                        tx: input.previous_output.hash,
                                                        index: input.previous_output.index
                                                    });
                                                else {
                                                    winston.error('spending output', {
                                                        topic: "output",
                                                        message: "output not spendable",
                                                        spending_tx: input.tx,
                                                        spending_index: input.index,
                                                        tx: input.previous_output.hash,
                                                        index: input.previous_output.index
                                                    });
                                                    // throw Error("ERR_SPENDING_OUTPUT");
                                                }
                                            });
                                    return {};
                                })))
                                .then(() => {
                                    if (number % 100000 == 0)
                                        Messenger.send('Sync milestone', `Block #${number} reached`);
                                    winston.info('block added', {
                                        topic: "block",
                                        message: "added",
                                        height: number,
                                        hash: header.hash
                                    });
                                })
                                .then(() => 0); //0 blocks orphan
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
            output.attachment.decimals = output.attachment.decimal_number;
            delete output.attachment.decimal_number;
            output.attachment.issue_tx = tx.hash;
            output.attachment.issue_index = output.index;
            output.attachment.height = tx.height;
            output.attachment.confirmed_at = tx.confirmed_at;
            if(output.attachment.is_secondaryissue) {
                if(output.attenuation_model_param) {
                    output.attachment.attenuation_model_param = output.attenuation_model_param;
                }
                secondaryIssue(output.attachment);
            } else {
                output.attachment.original_quantity = output.attachment.quantity;
                output.attachment.updates = [];
                newAsset(output.attachment);
            }
            return output;
        } else if (output.attachment.type == "asset-transfer") {
            return MongoDB.getAsset(output.attachment.symbol)
                .then((asset) => {
                    output.attachment.decimals = asset.decimals;
                    return output;
                });
        } else if (output.attachment.type == "did-register") {
            output.attachment.issue_tx = tx.hash;
            output.attachment.issue_index = output.index;
            output.attachment.height = tx.height;
            output.attachment.original_address = output.attachment.address;
            output.attachment.updates = [];
            output.attachment.confirmed_at = tx.confirmed_at;
            newAvatar(output.attachment);
            return output;
        } else if (output.attachment.type == "did-transfer") {
            output.attachment.issue_tx = tx.hash;
            output.attachment.issue_index = output.index;
            output.attachment.height = tx.height;
            output.attachment.confirmed_at = tx.confirmed_at;
            newAvatarAddress(output.attachment);
            return output;
        } else if (output.attachment.type == "asset-cert" || output.attachment.type == "mit") {
            return output;
        } else {
            //not handled type of TX
            Messenger.send('Unknow type', `Unknow output type in block ${tx.height}, transaction ${tx.hash}, index ${output.index}`);
            console.log('Unknown output type in blocks %i, transaction %i, index %i', tx.height, tx.hash, output.index);
            winston.warn('unknow type', {
                topic: "transaction",
                message: "unknown output type",
                height: tx.height,
                hash: tx.hash,
                block: tx.block,
                index: output.index,
                type: (output.attachment)?output.attachment.type:'none'
            });
            return output;
        }
    }));
}

function organizeTxPreviousOutputs(input) {
    return MongoDB.getTx(input.previous_output.hash)
        .then((previousTx) => {
            if (previousTx)
                return previousTx;
            else {
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
            if (previousOutput.attachment.type == "etp" || previousOutput.attachment.type == "message") {
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
            } else if (previousOutput.attachment.type == "did-register") {
                input.attachment.address = previousOutput.attachment.address;
                input.attachment.symbol = previousOutput.attachment.symbol;
                return input;
            }  else if (previousOutput.attachment.type == "did-transfer") {
                input.attachment.address = previousOutput.attachment.address;
                input.attachment.symbol = previousOutput.attachment.symbol;
                return input;
            } else if (previousOutput.attachment.type == "asset-cert") {
                input.attachment.to_did = previousOutput.attachment.to_did;
                input.attachment.symbol = previousOutput.attachment.symbol;
                input.attachment.cert = previousOutput.attachment.cert;
                return input;
            } else if (previousOutput.attachment.type == "mit") {
                input.attachment.to_did = previousOutput.attachment.to_did;
                input.attachment.symbol = previousOutput.attachment.symbol;
                input.attachment.status = previousOutput.attachment.status;
                return input;
            } else {
                //not handled type of TX
                Messenger.send('Unknow type', `Unknow output type in block ${previousTx.height}, transaction ${previousTx.hash}, index ${input.previous_output.index}`);
                console.log('Unknown output type in blocks %i, transaction %i, index %i', previousTx.hash, previousTx.height, input.previous_output.index);
                winston.warn('unknow type', {
                    topic: "transaction",
                    message: "unknown output type",
                    height: previousTx.height,
                    hash: previousTx.hash,
                    block: previousTx.block,
                    index: input.previous_output.index,
                    type: (previousOutput.attachment)?previousOutput.attachment.type:'none'
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

function newAsset(attachment) {
    return MongoDB.addAsset(attachment);
}

function secondaryIssue(attachment) {
    return MongoDB.secondaryIssue(attachment);
}

function newAvatar(attachment) {
    return MongoDB.addAvatar(attachment);
}

function newAvatarAddress(attachment) {
    return MongoDB.modifyAvatarAddress(attachment);
}

function detectFork(number, hash, forkhead, is_fork) {
    return MongoDB.getBlockByNumber(number)
        .then((block) => {
            if (block && block.hash != hash) {
                if (!is_fork) {
                    Messenger.send('Fork Detected', `Detected fork on block ${block.hash}`);
                    console.log('fork detected!!!');
                    winston.warn('fork detected', {
                        topic: "fork",
                        message: "blockchain forked",
                        height: block.number,
                        block: block.hash
                    });
                }
                return Mvsd.getBlock(number - 1)
                    .then((previousBlock) => detectFork(number - 1, previousBlock.hash, (forkhead) ? forkhead : block.hash, true));
            } else {
                if (!is_fork)
                    return null;
                else
                    return applyFork(number, forkhead);
            }
        });
}

function applyFork(number, forkhead) {
    return MongoDB.markOrphanFrom(number, forkhead)
        .then((forksize) => {
            Messenger.send('Fork Resolved', `Forked ${forksize} blocks from ${number} to block ${forkhead}`);
            console.log('forked %i blocks from %i to block %s', forksize, number, forkhead);
            winston.warn('fork resolved', {
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
            Messenger.send('Sync start', 'sync starting from block ' + lastblock.number);
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
            .then(() => applyFork((lastblock) ? lastblock.number : 0, "S"+Math.random()*1000000))
            .then(() => syncBlocksFrom((lastblock!==undefined) ? lastblock.number : 0));
    })
    .catch((error) => {
        console.error(error);
        Messenger.send('Sync exit', error.message);
        winston.error('sync error', {
            topic: "sync",
            message: error.message,
            details: error
        });
        return MongoDB.disconnect();
    });
