let requestify = require('requestify');

let url = 'http://127.0.0.1:8820/rpc/v2';

let service = {
    getBlock: getBlock,
    getTx: getTx
};

function getBlock(number) {
    return requestify.post(url, {
            "jsonrpc": "2.0",
            "method": "getblock",
            "params": [number,
                {
                    "json": true
                }
            ],
            "id": 27
    }, {dataType: 'json'})
        .then((response) => {
            response = JSON.parse(response.getBody());
            if (response.error != undefined && response.error.code) {
                console.error(response.error.message);
                throw Error(response.error.code);
            } else {
                return response.result;
            }
        });
}

function getTx(hash, json) {
    return requestify.post(url, {
            "jsonrpc": "2.0",
            "method": "gettx",
            "params": [hash,
                {
                    "json": json
                }
            ],
            "id": 27
    }, {dataType: 'json'})
        .then((response) => {
            response = JSON.parse(response.getBody());
            if (response.error != undefined && response.error.code) {
                console.error(response.error.message);
                throw Error(response.error.code);
            } else {
                return response.result;
            }
        });
}

module.exports = service;
