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
    removeBlock: removeBlock,
    getLastBlock: getLastBlock,
    getBlock: getBlock,
    getTx: getTx,
    markOrphanFrom: markOrphanFrom,
    markSpentOutput: markSpentOutput,
    getBlockByNumber: getBlockByNumber,
    addAvatar: addAvatar,
    getAvatar: getAvatar,
    modifyAvatarAddress: modifyAvatarAddress
};

function initPools() {
    return database.collection('pool')
        .createIndex({
            name: 1
        }, {
            unique: true
        })
        .then(() => {
            try {
                database.collection('pool').insert([{
                        name: "uupool",
                        url: 'http://www.uupool.cn',
                        origin: 'China',
                        addresses: ['M97VaaTYnKtGzfMFrreitiYoy84Eueb16N']
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
                        addresses: ['M8vkrEVPJCDn54L3TN64W3Grrq3KkBNVXh']
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
                        addresses: ['MEVgP8kvucyR9523t71FVKSiZccQjeK4ki']
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
                        name: "mvs",
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
                        name: "sandpool.org",
                        url: 'http:/etp.sandpool.org',
                        origin: 'Europe',
                        addresses: ['MWHLJTawEecdiz8xBK98GB6MGD1qzYRmrP']
                    }
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
        })
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
        })
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

function removeBlock(hash) {
    return new Promise((resolve, reject) => {
        database.collection('block').remove({
            hash: hash
        }, (err, block) => {
            if (err) throw err.message;
            else
                resolve(block);
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

function addBlock(header) {
    return database.collection('block').insertOne(header);
}

function addOutputs(outputs) {
    return database.collection('output').insertMany(outputs);
}

function addTx(tx) {
    return database.collection('tx').insertOne(tx);
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

function markOrphanFrom(number, forkhead) {
    let now = Math.floor(Date.now() / 1000);
    return Promise.all([
            markOrphanBlocksFrom(number, forkhead),
            markOrphanOutputsFrom(number, now),
            markOrphanTxsFrom(number),
            markUnspentOutputFrom(number)
        ])
        .then((results) => results[0]);
}

function markOrphanBlocksFrom(number, forkhead) {
    return new Promise((resolve, reject) => {
        database.collection('block').updateMany({
            number: {
                $gt: number
            },
            orphan: 0
        }, {
            $set: {
                orphan: forkhead
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                setTimeout(() => resolve(result.result.nModified), 10000);
        });
    });
}

function markOrphanTxsFrom(number) {
    return new Promise((resolve, reject) => {
        database.collection('tx').updateMany({
            height: {
                $gt: number
            },
            orphed_at: 0
        }, {
            $set: {
                orphan: 1
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                setTimeout(() => resolve(result.result.nModified), 10000);
        });
    });
}

function markSpentOutput(spending_tx, spending_index, height, spent_tx, spent_index) {
    return new Promise((resolve, reject) => {
        database.collection('output').updateMany({
            orphaned_at: 0,
            tx: spent_tx,
            index: spent_index
        }, {
            $set: {
                spent_tx: spending_tx,
                spent_index: spending_index,
                spent_height: height
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result.result.nModified);
        });
    });
}

/**
 * Fork handling to make outputs spendable.
 */
function markUnspentOutputFrom(start_height) {
    return new Promise((resolve, reject) => {
        database.collection('output').updateMany({
            spent_height: {
                $gt: start_height
            },
            orphaned_at: 0
        }, {
            $set: {
                spent_tx: 0,
                spent_index: null,
                spent_height: null
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result.result.nModified);
        });
    });
}

function markOrphanOutputsFrom(height, timestamp = 1) {
    return new Promise((resolve, reject) => {
        database.collection('output').remove({
            height: {
                $gt: height
            },
            orphaned_at: 0
        }, (err, result) => {
            if (err) throw err.message;
            else
                resolve(result.result.nRemoved);
        });
    });
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

function connect(url, name) {
    return new Promise((resolve, reject) => {
        MongoClient.connect(url, {
            w: 1,
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

function prepareStats(to_block) {
    return getConfig('address_balances')
        .then((config) => {
            if (!config)
                config = {
                    setting: 'address_balances'
                };
            if (!config.latest_block)
                config.latest_block = -1;
            if (to_block < config.latest_block)
                throw Error('ERR_PREPARE_ADDRESS_STATISTICS');
            else {
                return database.collection('tx').mapReduce(function() {
                            this.inputs.forEach((input) => {
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
                                } else if (input && input.previous_output.hash == "0000000000000000000000000000000000000000000000000000000000000000")
                                    emit("coinbase", {
                                        "ETP": -this.outputs[0].value
                                    });
                            });
                            this.outputs.forEach((output) => {
                                if (output && output.address) {
                                    if (output.value)
                                        emit(output.address, {
                                            "ETP": output.value
                                        });
                                    switch (output.attachment.type) {
                                        case 'asset-transfer':
                                        case 'asset-issue':
                                            if (output.attachment.symbol && output.attachment.symbol !== "ETP")
                                                emit(output.address, {
                                                    [output.attachment.symbol.replace(/\./g, '_')]: output.attachment.quantity
                                                });
                                            break;
                                    }
                                }
                            });
                        },
                        function(address, values) {
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
                            }
                        }
                    )
                    .then(() => {
                        config.latest_block = to_block;
                        return database.collection('config').update({
                            'setting': 'address_balances'
                        }, config, {
                            upsert: true
                        });
                    });
            }
        });
}

module.exports = service;
