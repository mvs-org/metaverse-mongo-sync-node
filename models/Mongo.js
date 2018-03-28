let MongoClient = require('mongodb').MongoClient;
let Promise = require('bluebird');

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

function init() {
    return connect('mongodb://127.0.0.1:27017', 'metaverse')
        .then(() =>
            Promise.all([
                database.createCollection('block'),
                database.collection('block').createIndex({
                    hash: 1,
                    block: 1
                }, {
                    unique: true
                }),
                database.collection('block').createIndex({
                    number: -1,
                    orphan: 1
                }),
                database.createCollection('tx'),
                database.collection('tx').createIndex({
                    hash: 1,
                    block: 1
                }, {
                    unique: true
                }),
                database.collection('tx').createIndex({
                    hash: 1,
                    orphan: 1
                }),
                database.collection('tx').createIndex({
                    height: -1
                }),
                database.createCollection('assets'),
            ])
        );
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
    return database.collection('assets').insertOne(asset);
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
            hash: hash,
            orphan: 0
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
        database.collection('assets').findOne({
            symbol: asset
        }, (err, symbol) => {
            if (err) throw err.message;
            else
                resolve(asset);
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
