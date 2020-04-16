let Mvsd = require('./models/Mvsd.js'),
    Messenger = require('./models/Messenger'),
    MongoDB = require('./models/Mongo.js'),
    Metaverse = require('metaversejs');

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

const PREPARE_STATS = (process.env.PREPARE_STATS) ? parseInt(process.env.PREPARE_STATS) : 1
const PREPARE_STATS_CHUNKSIZE = (process.env.PREPARE_STATS_CHUNKSIZE) ? parseInt(process.env.PREPARE_STATS_CHUNKSIZE) : 10000
const PREPARE_STATS_INTERVAL = (process.env.PREPARE_STATS_INTERVAL) ? parseInt(process.env.PREPARE_STATS_INTERVAL) : 10
const PREPARE_STATS_THRESHOLD = (process.env.PREPARE_STATS_THRESHOLD) ? parseInt(process.env.PREPARE_STATS_THRESHOLD) : 200

const NETWORK = process.env.NETWORK || 'MAINNET' 

const HARDFORK_SUPERNOVA = NETWORK === 'MAINNET' ? 1270000 : -100

const INTERVAL_BLOCK_RETRY = 5000

var avatarFromAddress = {}
var poolFromAddress = {}

var initialSyncDone = false

async function syncBlocksFrom(start) {
    while (true) {
        try {
            let orphaned = await syncBlock(start);
            if (orphaned)
                start -= orphaned;
            else
                start++;
            let target = start - PREPARE_STATS_THRESHOLD
            let syncedTo = 0
            while (PREPARE_STATS && syncedTo < target && (start >= 1000) && (start % PREPARE_STATS_INTERVAL == 0))
                syncedTo = await MongoDB.prepareStats(start - PREPARE_STATS_THRESHOLD, PREPARE_STATS_CHUNKSIZE);
        } catch (error) {
            if (error.message == 5101) {
                initialSyncDone = true
                console.info('no more block found. retry in ' + INTERVAL_BLOCK_RETRY + 'ms');
                console.info('check mempool transactions');
                Mvsd.getMemoryPool()
                    .then(memorypool => {
                        console.info(`found ${memorypool.length} transactions in memory pool`);
                        memorypool.forEach(tx => {
                            MongoDB.existsTx(tx.hash)
                                .then(async exists => {
                                    if (!exists) {
                                        tx.orphan = -1;
                                        tx.received_at = Math.floor(Date.now() / 1000);
                                        await organizeTx(tx, false)
                                            .then((updatedTx) => MongoDB.addTx(updatedTx))
                                            .catch((e) => {
                                                winston.error('add transaction', {
                                                    topic: "transaction",
                                                    message: e.message,
                                                    height: tx.height,
                                                    hash: tx.hash,
                                                    block: -1
                                                });
                                                console.error(e);
                                            });
                                    }
                                });
                        });
                    });
                await wait(INTERVAL_BLOCK_RETRY);
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
    if(number == 3584831) {
        console.log("Skip block 3584831")
        return
    }
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
                                    tx.isMiningReward = isMiningRewardTx(tx)
                                    tx.block = header.hash;
                                    tx.confirmed_at = header.time_stamp;
                                    return Promise.all(((tx.outputs) ? tx.outputs : []).map((output) => {
                                        output.tx = tx.hash;
                                        output.orphaned_at = 0;
                                        output.height = tx.height;
                                        output.spent_tx = 0;
                                        output.confirmed_at = tx.confirmed_at;
                                        if (output.attachment.type === 'message' && /^vote_([a-z0-9]+)\:([A-Za-z0-9-_@\.]+)$/.test(output.attachment.content)) {
                                            //this is a vote
                                            if (tx.voteIndex == undefined || tx.voteIndex < output.index) {
                                                tx.voteType = /^vote_([a-z0-9]+)\:/.test(output.attachment.content) ? output.attachment.content.match(/^vote_([a-z0-9]+)\:/)[1] : 'Invalid Type';
                                                tx.voteAvatar = /\:([A-Za-z0-9-_@\.]+)$/.test(output.attachment.content) ? output.attachment.content.match(/\:([A-Za-z0-9-_@\.]+)$/)[1] : 'Invalid Avatar';
                                                tx.voteIndex = output.index;
                                            }
                                        }
                                        if (Metaverse.script.isStakeLock(output.script)){
                                            output.locked_height_range = Metaverse.script.fromFullnode(output.script).getLockLength()
                                        } else if(tx.isMiningReward){
                                            // lock mining rewards for 1000 blocks
                                            output.locked_height_range = 1000
                                        }
                                        return output;
                                    }))
                                        .then((outputs) => {
                                            //Check if there was a vote in this transaction
                                            if (tx.voteIndex !== undefined) {
                                                outputs.forEach(output => {
                                                    if (outputIsVote(output)) {
                                                        output.vote = {
                                                            type: tx.voteType,
                                                            candidate: tx.voteAvatar,
                                                            lockedUntil: tx.height + output.attenuation_model_param.lock_period,
                                                        }
                                                        winston.info('new vote', {
                                                            topic: "vote",
                                                            message: "new vote",
                                                            tx: tx.hash,
                                                            candidate: output.vote.candidate,
                                                            symbol: output.attachment.symbol,
                                                            quantity: output.attachment.quantity,
                                                            lockedUntil: tx.height + output.attenuation_model_param.lock_period,
                                                            index: output.index,
                                                        });
                                                    }
                                                })
                                            }
                                            delete tx.voteAvatar;
                                            delete tx.voteIndex;
                                            delete tx.voteType;
                                            return MongoDB.addOutputs(outputs)
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
                                                })
                                        })
                                        .then(() => Promise.all(tx.inputs.map(async (input, index) => {
                                            input.tx = tx.hash;
                                            input.index = index;
                                            inputs.push(input);
                                            if (initialSyncDone) {
                                                await MongoDB.markTxsAsDoubleSpendThatHasInput(input.previous_output.hash, input.previous_output.index, tx.hash)
                                            }
                                            return input;
                                        })))
                                        .then(() => organizeTx(tx, true))
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
                                    .then(() => organizeBlockHeader(header, block.txs.transactions))
                                    .then((updatedHeader) => MongoDB.addBlock(updatedHeader))
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

function organizeTxOutputs(tx, outputs, add_entities) {
    return Promise.all(outputs.map((output) => {
        switch (output.attachment.type) {
            case "etp":
                if (Metaverse.script.isStakeLock(output.script))
                    output.locked_height_range = Metaverse.script.fromFullnode(output.script).getLockLength()
            case "message":
                output.attachment.symbol = "ETP";
                output.attachment.decimals = 8;
                return output;
            case "asset-issue":
                output.attachment.decimals = output.attachment.decimal_number;
                delete output.attachment.decimal_number;
                output.attachment.issue_tx = tx.hash;
                output.attachment.issue_index = output.index;
                output.attachment.height = tx.height;
                output.attachment.confirmed_at = tx.confirmed_at;
                outputs.forEach(other_output => {
                    if (other_output.attachment.cert == "mining" && output.attachment.symbol == other_output.attachment.symbol)
                        output.attachment.mining_model = other_output.attachment.content
                })
                if (output.attachment.is_secondaryissue) {
                    if (output.attenuation_model_param) {
                        output.attachment.attenuation_model_param = output.attenuation_model_param;
                    }
                    if (add_entities)
                        secondaryIssue(output.attachment);
                } else {
                    output.attachment.original_quantity = output.attachment.quantity;
                    output.attachment.updates = [];
                    if (add_entities)
                        newAsset(output.attachment);
                }
                return output;
            case "asset-transfer":
                return MongoDB.getAsset(output.attachment.symbol)
                    .then((asset) => {
                        output.attachment.decimals = asset.decimals;
                        return output;
                    });
            case "did-register":
                output.attachment.issue_tx = tx.hash;
                output.attachment.issue_index = output.index;
                output.attachment.height = tx.height;
                output.attachment.original_address = output.attachment.address;
                output.attachment.updates = [];
                output.attachment.confirmed_at = tx.confirmed_at;
                if (add_entities)
                    newAvatar(output.attachment);
                return output;
            case "did-transfer":
                output.attachment.issue_tx = tx.hash;
                output.attachment.issue_index = output.index;
                output.attachment.height = tx.height;
                output.attachment.confirmed_at = tx.confirmed_at;
                if (add_entities)
                    newAvatarAddress(output.attachment);
                return output;
            case "asset-cert":
            case "mit":
            case "coinstake":
                return output;
            default:
                //not handled type of TX
                Messenger.send('Unknow type', `Unknow output type in block ${tx.height}, transaction ${tx.hash}, index ${output.index}`);
                console.log('Unknown output type %s in blocks %i, transaction %i, index %i', tx.height, tx.hash, output.index, output.attachment.type);
                winston.warn('unknow type', {
                    topic: "transaction",
                    message: "unknown output type",
                    height: tx.height,
                    hash: tx.hash,
                    block: tx.block,
                    index: output.index,
                    type: (output.attachment) ? output.attachment.type : 'none'
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
            input.attachment.type = previousOutput.attachment.type;
            input.value = previousOutput.value;
            input.address = previousOutput.address;
            switch (previousOutput.attachment.type) {
                case "etp":
                case "message":
                    input.attachment.symbol = "ETP";
                    input.attachment.decimals = 8;
                    return input;
                case "asset-issue":
                    input.attachment.quantity = previousOutput.attachment.quantity;
                    input.attachment.symbol = previousOutput.attachment.symbol;
                    input.attachment.decimals = previousOutput.attachment.decimals;
                    return input;
                case "asset-transfer":
                    return MongoDB.getAsset(previousOutput.attachment.symbol)
                        .then((asset) => {
                            input.attachment.quantity = previousOutput.attachment.quantity;
                            input.attachment.symbol = previousOutput.attachment.symbol;
                            input.attachment.decimals = asset.decimals;
                            return input;
                        });
                case "did-transfer":
                    input.attachment.address = previousOutput.attachment.address;
                    input.attachment.symbol = previousOutput.attachment.symbol;
                    return input;
                case "asset-cert":
                    input.attachment.to_did = previousOutput.attachment.to_did;
                    input.attachment.symbol = previousOutput.attachment.symbol;
                    input.attachment.cert = previousOutput.attachment.cert;
                    return input;
                case "mit":
                    input.attachment.to_did = previousOutput.attachment.to_did;
                    input.attachment.symbol = previousOutput.attachment.symbol;
                    input.attachment.status = previousOutput.attachment.status;
                    return input;
                default:
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
                        type: (previousOutput.attachment) ? previousOutput.attachment.type : 'none'
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

function organizeTx(tx, add_entities) {
    return Promise.all([
        organizeTxOutputs(tx, tx.outputs, add_entities),
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

function organizeBlockHeader(header, txs) {
    txs.forEach(tx => {
        if (isMiningRewardTx(tx)) {
            header.miner_address = tx.outputs[0].address;
            if (tx.outputs[1])
                header.mst_mining = tx.outputs[1].attachment.symbol;
            switch (header.version) {
                case 1:
                    if (poolFromAddress[tx.outputs[0].address]) {
                        header.miner = poolFromAddress[tx.outputs[0].address];
                        winston.info('miner detected', {
                            topic: "block",
                            message: "miner",
                            height: header.number,
                            type: 'pow',
                            miner: header.miner,
                            address: header.miner_address,
                            hash: header.hash
                        });
                    } else {
                        winston.info('solo miner', {
                            topic: "block",
                            message: "solo miner",
                            height: header.number,
                            type: 'pow',
                            address: header.miner_address,
                            hash: header.hash
                        });
                    }
                    return header;
                case 2:
                    header.miner = avatarFromAddress[tx.outputs[0].address];
                    winston.info('miner detected', {
                        topic: "block",
                        message: "miner",
                        height: header.number,
                        type: 'pos',
                        miner: header.miner,
                        address: header.miner_address,
                        hash: header.hash
                    });
                    return header;
                default:
                    return header;
            }
        }
    })
    return header;
}

function getAllPools() {
    return MongoDB.getAllPools()
        .then((pools) => {
            pools.forEach((pool) => {
                pool.addresses.forEach((address) => {
                    poolFromAddress[address] = pool.name;
                })
            })
            return
        })
}

function getAllAvatars() {
    return MongoDB.getAllAvatars()
        .then((avatars) => {
            avatars.forEach((avatar) => {
                avatarFromAddress[avatar.address] = avatar.symbol;
            })
            return
        })
}

function newAsset(attachment) {
    return MongoDB.addAsset(attachment);
}

function secondaryIssue(attachment) {
    return MongoDB.secondaryIssue(attachment);
}

function newAvatar(attachment) {
    avatarFromAddress[attachment.address] = attachment.symbol
    return MongoDB.addAvatar(attachment);
}

function newAvatarAddress(attachment) {
    avatarFromAddress[attachment.address] = attachment.symbol
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
                    .then((previousBlock) => detectFork(number - 1, previousBlock.header.result.hash, (forkhead) ? forkhead : block.hash, true));
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

function outputIsVote(output) {
    return output.attachment.type === 'asset-transfer' &&
        output.attenuation_model_param !== undefined &&
        output.attenuation_model_param.total_period_nbr == 1 &&
        output.attenuation_model_param.type == 1
}

function depositEnabled(height){
    return height < HARDFORK_SUPERNOVA
}

function isMiningRewardTx(tx){
    if(tx.isMiningReward!==undefined) return tx.isMiningReward
    return tx.inputs[0].previous_output.hash == "0000000000000000000000000000000000000000000000000000000000000000" 
            && tx.inputs[0].script != '' 
            && !(depositEnabled(tx.height) && tx.outputs[0].locked_height_range !== 0)
}


MongoDB.init()
    .then(() => Promise.all([getAllAvatars(), getAllPools()]))
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
        if (lastblock !== undefined)
            return MongoDB.clearDataFrom(lastblock.number).then(() => lastblock.number);
        else
            return Promise.resolve(0);
    })
    .then((height) => syncBlocksFrom(height))
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
