let MongoClient = require('mongodb').MongoClient;
let Promise = require('bluebird');

let client = null;
let database = null;

let service = {
    init: init,
    disconnect: disconnect,
    addTx: addTx,
    addBlock: addBlock,
    removeBlock: removeBlock,
    getLastBlock: getLastBlock,
    getBlock: getBlock,
    markOrphanFrom: markOrphanFrom,
    markOutputsAsSpent: markOutputsAsSpent,
    includeInputsAddresses: includeInputsAddresses,
    getBlockByNumber: getBlockByNumber
};

function init() {
    return connect('mongodb://127.0.0.1:27017', 'metaverse')
        .then(() =>
            Promise.all([
                database.createCollection('block'),
                database.collection('block').createIndex({
                    hash: 1
                }, {
                    unique: true
                }),
                database.collection('block').createIndex({
                    number: -1,
                    orphan: 1
                }),
                database.createCollection('tx'),
                database.collection('tx').createIndex({
                    hash: 1
                }, {
                    unique: true
                }),
                database.collection('tx').createIndex({
                    height: -1
                }),
            ])
        );
}

function removeBlock(hash){
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

function markOutputsAsSpent(tx){
    return Promise.all(tx.inputs.map((input, index) => {
        if (input.previous_output.index < 4294967295)
            return database.collection('tx').find({
                hash: input.previous_output.hash
            }).toArray().then((input_txs) => {
                if(!input_txs.length){
                    console.error("couldnt find %s %s for %s",input.previous_output.hash, input.previous_output.index, tx.hash);
                    process.exit();
                }
                let input_tx = input_txs[0];
                input_tx.outputs[input.previous_output.index].spent_in = {
                    hash: tx.hash,
                    index: index
                };
                return database.collection('tx').update({
                    hash: input_tx.hash
                }, input_tx).then(() => console.info('maked output %s %i as spent', input_tx.hash, index));
            });
        else return null; //Coinbase input
    }));
}

function includeInputsAddresses(tx){
    return Promise.all(tx.inputs.map((input, index) => {
        if (input.previous_output.index < 4294967295)
            return database.collection('tx').find({
                hash: input.previous_output.hash
            }).toArray().then((input_txs) => {
                if(!input_txs.length){
                    console.error("couldnt find %s %s for %s",input.previous_output.hash, input.previous_output.index, tx.hash);
                    process.exit();
                }
                let input_tx = input_txs[0];
                input.address = input_tx.address;
                return database.collection('tx').update({
                    hash: input.hash
                }, input).then(() => console.info('added input address from %s %i', input_tx.hash, index));
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

function markOrphanFrom(number, forkhead){
    //TODO Mark outpus of forked txs as spent
    return new Promise((resolve, reject) => {
        database.collection('block').updateMany({
            number: { $gt: number-1},
            orphan: 0
        },{$set: {orphan: forkhead}}, (err, result) => {
            if (err) throw err.message;
            else
                setTimeout(()=>resolve(result.result.nModified),10000);
        });
    });
}

function connect(url, name) {
    return new Promise((resolve, reject) => {
        MongoClient.connect(url, {w: 1, j: false}, (err, con) => {
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
