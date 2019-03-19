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
                    addresses: ['MULT79mjG6qp31pEAVD5QzQAMUoEFKSNzR']
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

function addBlock(header) {
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

function markOrphanFrom(number, forkhead) {
    let now = Math.floor(Date.now() / 1000);
    return Promise.all([
        markOrphanBlocksFrom(number, forkhead),
        removeOutputsFrom(number, now),
        markOrphanTxsFrom(number),
        getConfig('address_balances').then((c) => {
            // Check if the calculated address balances are affected by the fork
            if (c && c.latest_block && c.latest_block < number) {
                console.info(`no address balance recalculation needed. configuration height ${c.latest_block} compared to fork height ${number}`)
                return
            }
            console.info(`address balance configuration height ${c ? c.latest_block : undefined} compared to fork height ${number}`)
            return resetStats()
        }),
        markUnspentOutputFrom(number)
    ])
        .then((results) => results[0]);
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
                if (err) throw Error(err.message);
                else
                    resolve(result.result.nModified);
            });
    });
}

function markOrphanTxsFrom(number, fork) {
    return new Promise((resolve, reject) => {
        database.collection('tx').updateMany({
            height: {
                $gt: number
            },
            orphan: 0
        }, {
                $set: {
                    orphan: 1
                }
            }, (err, result) => {
                if (err) throw Error(err.message);
                else
                    resolve(result.result.nModified);
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
                $gte: start_height
            }
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

function removeOutputsFrom(height) {
    return new Promise((resolve, reject) => {
        database.collection('output').deleteMany({
            height: {
                $gte: height
            }
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

module.exports = service;
