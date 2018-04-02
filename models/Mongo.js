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
    getAsset: getAsset,
    removeBlock: removeBlock,
    getLastBlock: getLastBlock,
    getBlock: getBlock,
    getTx: getTx,
    markOrphanFrom: markOrphanFrom,
    markOutputsAsSpent: markOutputsAsSpent,
    getBlockByNumber: getBlockByNumber
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
            height: -1
        })
    ]);
}
function init() {
    return connect('mongodb://'+config.host+':'+config.port, config.database)
        .then(() => initPools())
        .then(() => initBlocks())
        .then(() => initTxs());
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

function addTx(tx) {
    return database.collection('tx').insertOne(tx);
}

function addAsset(asset) {
    return database.collection('asset').insertOne(asset);
}

function markOutputsAsSpent(tx) {
    return Promise.all(tx.inputs.map((input, index) => {
        if (input.previous_output.index < 4294967295)
            return database.collection('tx').find({
                hash: input.previous_output.hash
            }).toArray().then((input_txs) => {
                if (!input_txs.length) {
                    console.error("couldnt find %s %s for %s", input.previous_output.hash, input.previous_output.index, tx.hash);
                    process.exit();
                }
                let input_tx = input_txs[0];
                input_tx.outputs[input.previous_output.index].spent_in = {
                    hash: tx.hash,
                    index: index
                };
                return database.collection('tx').update({
                    hash: input_tx.hash
                }, input_tx).then(() => console.info('marked output %s %i as spent', input_tx.hash, index));
            });
        else return null; //Coinbase input
    }));
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
    return Promise.all([
        markOrphanBlocksFrom(number, forkhead),
        markOrphanTxsFrom(number)
    ]);
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
            orphan: 0
        }, {
            $set: {
                orphan: 1
            }
        }, (err, result) => {
            if (err) throw err.message;
            else
                setTimeout(() => resolve(result.result.nModified), 10000);
        });
        //TODO Add the same logic for transaction
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

function connect(url, name) {
    return new Promise((resolve, reject) => {
        MongoClient.connect(url, {
            w: 1,
            j: false
        }, (err, con) => {
            if (err) throw err;
            else {
                client = con;
                database = con.db(name);
                resolve();
            }
        });
    });
}

module.exports = service;
