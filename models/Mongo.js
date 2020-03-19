let MongoClient = require('mongodb').MongoClient;
let Promise = require('bluebird');

let config = require('../config/mongo.js');
let client = null;
let database = null;

let service = {
    init: init,
    disconnect: disconnect,
    addTx: addTx,
    addBlock: addBlock,
    addAsset: addAsset,
    secondaryIssue: secondaryIssue,
    addOutputs: addOutputs,
    getAsset: getAsset,
    prepareStats: prepareStats,
    clearDataFrom: clearDataFrom,
    getLastBlock: getLastBlock,
    getBlock: getBlock,
    getTx: getTx,
    existsTx: existsTx,
    markOrphanFrom: markOrphanFrom,
    markSpentOutput: markSpentOutput,
    getBlockByNumber: getBlockByNumber,
    addAvatar: addAvatar,
    getAvatar: getAvatar,
    getAllAvatars: getAllAvatars,
    getAllPools: getAllPools,
    modifyAvatarAddress: modifyAvatarAddress,
    markTxsAsDoubleSpendThatHasInput: markTxsAsDoubleSpendThatHasInput
};

const DOUBLE_SPENT_DEPTH_HEIGHT = 20

function initPools() {
    return database.collection('pool')
        .createIndex({
            name: 1
        }, {
            unique: true
        })
        .then(() => {
            try {
                database.collection('pool').insertMany([{
                    name: "uupool",
                    url: 'http://www.uupool.cn',
                    origin: 'China',
                    addresses: ['M97VaaTYnKtGzfMFrreitiYoy84Eueb16N', 'MUiW2CViWLQBg2TQDsRt1Pcj7KyrdqFPj7']
                },
                {
                    name: "xinyuanjie",
                    url: 'http://xinyuanjie.org',
                    origin: 'China',
                    addresses: ['MKD5pmeyR14KGUxLo7YEECbgJ7MfrLPdTG']
                },
                {
                    name: "dodopool",
                    url: 'http://etp.dodopool.com',
                    origin: 'UK',
                    addresses: ['M8vkrEVPJCDn54L3TN64W3Grrq3KkBNVXh', 'MAXwUSQKHWxVYvpg4Cs3epPQTLKg14WjaB', 'MX2evLyh7pcW4SevTzs1CbyQCf9C8fPmWi']
                },
                {
                    name: "huopool",
                    url: 'http://cryptopoolpond.com/#/',
                    origin: 'China',
                    addresses: ['MGr3pfTK8qJx3JEVnkTBjUSQZVK4jDujpZ']
                },
                {
                    name: "metaverse.farm",
                    url: 'https://metaverse.farm',
                    origin: 'UK',
                    addresses: ['MEVgP8kvucyR9523t71FVKSiZccQjeK4ki', 'MSLiK7d6JcmH6WVaq73kv4hi5J3pJnzhTV']
                },
                {
                    name: "altpool.pro",
                    url: 'http://etp.altpool.pro',
                    origin: 'Europe',
                    addresses: ['M97sAWjC2Du6RNBaHiCoqZFAGYXWrTM9At']
                },
                {
                    name: "fairpool",
                    url: 'https://etp.fairpool.xyz',
                    origin: 'US',
                    addresses: ['MGodWFRV7wu3jzwqfDakSCoH6ouhBL2TVG']
                },
                {
                    name: "comining.io",
                    url: 'http://comining.io',
                    origin: 'Russia',
                    addresses: ['MNd7ZeeadbSzEAtXmQ7B1SsoEoi7kHUWPg']
                },
                {
                    name: "pool.mvs",
                    url: 'http://pool.mvs.live',
                    origin: 'China',
                    addresses: ['MFuFkdbp77YFQ24CnfLdLaKCdh9ASsR4r2']
                },
                {
                    name: "cryptopoolpond",
                    url: 'http://cryptopoolpond.com/#/',
                    origin: 'US/Europe',
                    addresses: ['MPVYH9GQGZkJc4rZ4ZEx9bnV1mpa3M4whw']
                },
                {
                    name: "europool",
                    url: 'https://etp.europool.me/',
                    origin: 'Europe',
                    addresses: ['MAt2UVEvSN6SrRyjwpJKHqXnXqXC3wWnRK']
                },
                {
                    name: "chileminers",
                    url: "http://etp.chileminers.cl",
                    origin: "US",
                    addresses: ['MJjMh7F2ZuNcEQvrEK2PoMwCyqX7vJrqTV', 'MKCLevqjbhyK7dMWT261uP8EaKzqkX1WUi']
                },
                {
                    name: "sandpool.org",
                    url: 'http:/etp.sandpool.org',
                    origin: 'Europe',
                    addresses: ['MWHLJTawEecdiz8xBK98GB6MGD1qzYRmrP', 'MCA8vo22N5w1KVNpytSW9Dc7wrDoxPRjNd']
                },
                {
                    name: "etphunter",
                    url: 'http://etphunter.com',
                    origin: 'Asia/Pacific',
                    addresses: ['M8N42NP5NR6tcgXqyzG3oZRqBqPoWRMLGu', 'MGvR5eJteEJaLHW8bfvV4uoPYamdpRqtst', 'MNDrZ8K5onqt6oJysurGkxG8Qq39aHV6uH']
                },
                {
                    name: "topminers",
                    url: 'https://topmining.co.kr/',
                    origin: 'Asia/Pacific',
                    addresses: ['MG65zQHtch4zxj9ghZKyTcjrRDiCdPAf8M']
                },
                {
                    name: "2miners",
                    url: 'https://2miners.com/',
                    origin: 'Europe',
                    addresses: ['MULT79mjG6qp31pEAVD5QzQAMUoEFKSNzR', 'MV35jawXLsNFM2MGMUKsJxoVsZx67oSJ6v']
                },
                {
                    name: "dpool",
                    url: 'https://www.dpool.top/',
                    origin: 'Asia/Pacific',
                    addresses: ['MJqCzYjQUDcrLR1aFQUa31YewGpjoMKVDR']
                },
                {
                    name: "xzrm",
                    url: 'http://xzrm.com/',
                    origin: 'China',
                    addresses: ['MJRr2iJjnexvDvPQKHUcLaPAy6Mtnz7Sve', 'MWhyyPmrKhEtrr6VLMCSRfsNdL5qacDSQn']
                },
                {
                    name: "madenim",
                    url: "https://www.madenim.org/",
                    origin: "Europe",
                    addresses: ["MScz3BTwujNXCJ89QcBGjKZ2BaJh4FrH5y"]
                },
                {
                    name: "f2pool",
                    url: "https://www.f2pool.com/",
                    origin: "China",
                    addresses: ["MTCZUqTpQPYmAK2GK2cyfu5jBhw8Mw3b5X"]
                },
                ]);
            } catch (e) {
                console.error(e);
            }
        });
}

function initBlocks() {
    return Promise.all([
        database.collection('block').createIndex({
            hash: 1,
            block: 1
        }, {
            unique: true
        }),
        database.collection('block').createIndex({
            number: -1,
            orphan: 1
        })
    ]);
}

function initOutputs() {
    return Promise.all([
        database.collection('output').createIndex({
            tx: 1,
            index: 1,
            orphaned_at: 1,
            spent_tx: 1,
            spent_index: 1
        }, {
            unique: true
        }),
        database.collection('output').createIndex({
            "attachment.type": 1,
            orphaned_at: 1,
            spent_tx: 1,
            height: -1
        }),
        database.collection('output').createIndex({
            height: -1,
            address: 1,
            orphaned_at: 1,
            spent_tx: 1
        }),
        database.collection('output').createIndex({
            height: 1,
            'vote.lockedUntil': 1,
            'vote.type': 1,
            'attachment.symbol': 1
        }),
    ]);
}

function initConfig() {
    return Promise.all([
        database.collection('config').createIndex({
            "setting": 1
        }, {
            unique: true
        })
    ]);
}

function initTxs() {
    return Promise.all([
        database.collection('tx').createIndex({
            "inputs.address": 1,
            "orphan": 1
        }),
        database.collection('tx').createIndex({
            "outputs.address": 1,
            "orphan": 1
        }),
        database.collection('tx').createIndex({
            hash: 1,
            block: 1
        }, {
            unique: true
        }),
        database.collection('tx').createIndex({
            hash: 1
        }),
        database.collection('tx').createIndex({
            block: 1
        }),
        database.collection('tx').createIndex({
            height: -1
        }),
        database.collection('tx').createIndex({
            hash: 1,
            'inputs.previous_output.hash': 1,
            'inputs.previous_output.index': 1,
        }),
        database.collection('tx').createIndex({
            hash: 1,
            'inputs.previous_output.hash': 1,
        }),
    ]);
}

function initAssets() {
    return Promise.all([
        database.collection('asset').createIndex({
            symbol: 1
        }, {
            unique: true
        })
    ]);
}

function initAvatars() {
    return Promise.all([
        database.collection('avatar').createIndex({
            symbol: 1
        }, {
            unique: true
        })
    ]);
}

function init() {
    return connect('mongodb://' + config.host + ':' + config.port, config.database)
        .then(() => initPools())
        .then(() => initBlocks())
        .then(() => initConfig())
        .then(() => initOutputs())
        .then(() => initTxs())
        .then(() => initAssets())
        .then(() => initAvatars());
}

function removeTxsFrom(start_height) {
    return new Promise((resolve, reject) => {
        database.collection('tx').deleteMany({
            height: {
                $gte: start_height
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result.result.nModified);
        });
    });
}

function removeBlocksFrom(start_height) {
    return new Promise((resolve, reject) => {
        database.collection('block').deleteMany({
            number: {
                $gte: start_height
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result.result.nModified);
        });
    });
}

function disconnect() {
    if (client == null)
        return null;
    return client.close()
        .then(() => {
            client = null;
            database = null;
            console.info('database connection closed');
        });
}

function getLastBlock() {
    return database.collection('block').find({
        orphan: 0
    }).sort({
        number: -1
    }).limit(1).toArray().then((result) => result[0]);
}

async function addBlock(header) {
    console.debug(`add block ${header.number}`)
    return database.collection('block').insertOne(header);
}

function addOutputs(outputs) {
    return database.collection('output').insertMany(outputs);
}

function addTx(tx) {
    return database.collection('tx').update({
        hash: tx.hash,
        "$or": [{
            block: tx.block
        }, {
            block: {
                $exists: false
            }
        }]
    }, tx, {
        upsert: true
    });
}

function addAsset(asset) {
    return database.collection('asset').insertOne(asset);
}

function secondaryIssue(asset) {
    return new Promise((resolve, reject) => {
        database.collection('asset').updateMany({
            symbol: asset.symbol
        }, {
            $inc: {
                quantity: asset.quantity
            },
            $push: {
                updates: asset
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result.result.nModified);
        });
    });
}

function addAvatar(avatar) {
    return database.collection('avatar').insertOne(avatar);
}

function modifyAvatarAddress(avatar) {
    return new Promise((resolve, reject) => {
        database.collection('avatar').updateMany({
            symbol: avatar.symbol
        }, {
            $set: {
                address: avatar.address
            },
            $push: {
                updates: avatar
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result.result.nModified);
        });
    });
}

function getBlock(hash) {
    return new Promise((resolve, reject) => {
        database.collection('block').findOne({
            hash: hash
        }, (err, block) => {
            if (err) throw err.message;
            else
                resolve(block);
        });
    });
}

function existsTx(hash) {
    return getTx(hash)
        .then(tx => {
            return (tx) ? true : false;
        });
}


function getTx(hash) {
    return new Promise((resolve, reject) => {
        database.collection('tx').findOne({
            hash: hash
        }, (err, block) => {
            if (err) throw err.message;
            else
                resolve(block);
        });
    });
}

function getBlockByNumber(number) {
    return new Promise((resolve, reject) => {
        database.collection('block').findOne({
            number: number,
            orphan: 0
        }, (err, block) => {
            if (err) throw err.message;
            else
                resolve(block);
        });
    });
}

async function markOrphanFrom(number, forkhead) {
    console.debug(`mark database records as orphan from block ${number} ${forkhead}`)
    const countUnspent = await markUnspentOutputFrom(number)
    console.debug(`marked ${countUnspent} outputs as unspent`)
    const countBlocks = await markOrphanBlocksFrom(number, forkhead)
    console.debug(`marked ${countBlocks} blocks as forked`)
    const countRemovedOutputs = await removeOutputsFrom(number)
    console.debug(`removed ${countRemovedOutputs} outputs`)
    const countTransactions = await markOrphanTxsFrom(number, forkhead)
    console.debug(`marked ${countTransactions} transactions as forked`)
    const config = await getConfig('address_balances')
    if (config && config.latest_block && config.latest_block < number) {
        console.info(`no address balance recalculation needed. configuration height ${config.latest_block} compared to fork height ${number}`)
    } else {
        console.info(`address balance configuration height ${config ? config.latest_block : undefined} compared to fork height ${number}`)
        await resetStats()
    }
    return countBlocks
}

function clearDataFrom(height) {
    console.info('clear from ' + height);
    return Promise.all([
        removeBlocksFrom(height),
        removeTxsFrom(height),
        resetStats(),
        removeOutputsFrom(height).then(() => markUnspentOutputFrom(height)),
    ])
        .then((results) => results[0]);
}

function markOrphanBlocksFrom(number, forkhead) {
    return database.collection('block').updateMany({
        number: {
            $gte: number
        },
        orphan: 0
    }, {
        $set: {
            orphan: forkhead
        }
    })
        .then((result) => result.result.nModified)
        .catch(error => {
            console.error(`failed to update reorged block: ${error.message}`)
            return 0
        })
}

function markOrphanTxsFrom(number, forkhead) {
    return database.collection('tx').updateMany({
        height: {
            $gte: number
        },
        orphan: 0
    }, {
        $set: {
            orphan: forkhead
        }
    })
        .then((result) => result.result.nModified)
        .catch(error => {
            console.error(`failed to mark orphan transactions: ${error.message}`)
            return 0
        })
}

function markSpentOutput(spending_tx, spending_index, height, spent_tx, spent_index) {
    return database.collection('output').updateMany({
        orphaned_at: 0,
        tx: spent_tx,
        index: spent_index
    }, {
        $set: {
            spent_tx: spending_tx,
            spent_index: spending_index,
            spent_height: height
        }
    })
        .then((result) => result.result.nModified)
}

/**
 * Fork handling to make outputs spendable.
 */
function markUnspentOutputFrom(start_height) {
    return database.collection('output').updateMany({
        spent_height: {
            $gte: start_height
        }
    }, {
        $set: {
            spent_tx: 0,
            spent_index: null,
            spent_height: null
        }
    })
        .then((result) => result.result.nModified)
}

function removeOutputsFrom(height) {
    return database.collection('output')
        .deleteMany({
            height: {
                $gte: height
            }
        })
        .then((result) => result.result.n)
}

function getAsset(asset) {
    return new Promise((resolve, reject) => {
        database.collection('asset').findOne({
            symbol: asset
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result);
        });
    });
}

function getAvatar(avatar) {
    return new Promise((resolve, reject) => {
        database.collection('avatar').findOne({
            symbol: avatar
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result);
        });
    });
}

function getAllAvatars() {
    return database.collection('avatar')
        .find({}).toArray().then((result) => result);
}

function getAllPools() {
    return database.collection('pool')
        .find({}).toArray().then((result) => result);
}

function connect(url, name) {
    return new Promise((resolve, reject) => {
        MongoClient.connect(url, {
            w: 1,
            connectTimeoutMS: 900000,
            j: false
        }, (err, con) => {
            if (err) {
                console.error(err);
                process.exit(1);
            } else {
                client = con;
                database = con.db(name);
                resolve();
            }
        });
    });
}

function getConfig(setting) {
    return database.collection('config').findOne({
        setting: setting
    });
}

function resetStats() {
    return database.collection('config').deleteMany({
        setting: 'address_balances'
    });
}

function resetAddressBalances() {
    return database.collection('address_balances').deleteMany({});
}

function prepareStats(to_block, chunksize) {
    return getConfig('address_balances')
        .then((config) => {
            if (!config) {
                config = {
                    setting: 'address_balances'
                };
                return resetAddressBalances().then(() => config);
            }
            return config;
        })
        .then(config => {
            if (!config.latest_block)
                config.latest_block = -1;
            to_block = Math.min(config.latest_block + chunksize, to_block)
            console.info(`start prepare statistics from ${config.latest_block} to block ${to_block}`)
            if (to_block < config.latest_block)
                throw Error('ERR_PREPARE_ADDRESS_STATISTICS');
            else {
                return database.collection('tx').mapReduce(function () {
                    this.inputs.forEach((input) => {
                        // check if input is not from coinbase
                        if (input.address !== "") {
                            if (input && input.value) {
                                emit(input.address, {
                                    "ETP": -input.value
                                });
                            }
                            switch (input.attachment.type) {
                                case 'asset-transfer':
                                case 'asset-issue':
                                    if (input.attachment.symbol && input.attachment.symbol !== "ETP") {
                                        emit(input.address, {
                                            [input.attachment.symbol.replace(/\./g, '_')]: -input.attachment.quantity
                                        });
                                    }
                                    break;
                            }
                        }
                    });
                    this.outputs.forEach((output) => {
                        const isCoinbase = this.inputs[0].previous_output.hash == "0000000000000000000000000000000000000000000000000000000000000000"
                        if (output && output.address) {
                            if (output.value) {
                                emit(output.address, {
                                    "ETP": output.value
                                });
                                // deduct any coinbase sourced etp from coinbase record
                                if (isCoinbase) {
                                    emit("coinbase", {
                                        "ETP": -output.value
                                    });
                                }
                            }
                            switch (output.attachment.type) {
                                case 'asset-transfer':
                                case 'asset-issue':
                                    if (output.attachment.symbol && output.attachment.symbol !== "ETP") {
                                        emit(output.address, {
                                            [output.attachment.symbol.replace(/\./g, '_')]: output.attachment.quantity
                                        });
                                        // deduct any coinbase sourced assets from coinbase record
                                        if (isCoinbase) {
                                            emit('coinbase', {
                                                [output.attachment.symbol.replace(/\./g, '_')]: -output.attachment.quantity
                                            });
                                        }
                                    }
                                    break;
                            }
                        }
                    });
                },
                    function (address, values) {
                        var result = {};
                        values.forEach((item) => {
                            Object.keys(item).forEach((symbol) => {
                                if (result[symbol]) {
                                    result[symbol] += item[symbol];
                                } else
                                    result[symbol] = item[symbol];
                            });
                        });
                        return result;
                    }, {
                    out: {
                        reduce: 'address_balances'
                    },
                    query: {
                        orphan: 0,
                        height: {
                            $gt: config.latest_block,
                            $lte: to_block
                        }
                    },
                    sort: { height: 1 }
                }
                )
                    .then(() => {
                        config.latest_block = to_block;
                        console.info('prepare statistics finished to block ' + to_block)
                        return database.collection('config').update({
                            'setting': 'address_balances'
                        }, config, {
                            upsert: true
                        });
                    })
                    .then(() => to_block)

            }
        });
}

function getTransactionsForPreviousTransactionHash(sourceTx, previousOutputHash, previousOutputIndex) {
    if (previousOutputHash === '0000000000000000000000000000000000000000000000000000000000000000')
        return Promise.resolve([])
    return database.collection('tx').find({
        'hash': { $ne: sourceTx },
        'inputs.previous_output.hash': previousOutputHash,
        ...(previousOutputIndex !== undefined && { 'inputs.previous_output.index': previousOutputIndex }),
    }).toArray()
}

async function markTxsAsDoubleSpendThatHasInput(previousOutputHash, previousOutputIndex, sourceTx, level) {
    if (level === undefined) {
        level = 0
    } else if (level > DOUBLE_SPENT_DEPTH_HEIGHT) {
        console.error('maximum double spent detection depth reached')
    }
    const targetTxs = await getTransactionsForPreviousTransactionHash(sourceTx, previousOutputHash, previousOutputIndex)
    if (targetTxs.length) {
        const txsUpdate = await database.collection('tx').updateMany({
            'hash': { $ne: sourceTx },
            'inputs.previous_output.hash': previousOutputHash,
            ...(previousOutputIndex !== undefined && { 'inputs.previous_output.index': previousOutputIndex }),
        }, {
            $set: {
                'double_spent': 1,
            }
        })
        console.log(`marked ${txsUpdate.modifiedCount} transactions as double spent by ${sourceTx} level ${level}`)
        for (tx of targetTxs) {
            await markTxsAsDoubleSpendThatHasInput(sourceTx, undefined, tx.hash, level + 1)
        }
    }
}

module.exports = service;
