let requestify = require('requestify');
let config = require('../config/mvsd.js');

let url = 'http://'+config.host+':'+config.port+'/rpc/v2';

let service = {};

service.getBlock = function(number) {
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
        .then((response) => parseResponse(response));
};

service.getMemoryPool = function() {
    return requestify.post(url, {
        "jsonrpc": "2.0",
        "method": "getmemorypool",
        "params": [
                   {
                       "json": true
                   }
                  ],
        "id": 27
    }, {dataType: 'json'})
        .then((response) => parseResponse(response))
        .then(response=>(response.transactions!==null)?response.transactions:[]);

};

service.getTx = function(hash, json) {
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
        .then((response) => parseResponse(response));
};

function parseResponse(response){
    response = JSON.parse(response.getBody());
    if (response.error != undefined && response.error.code) {
        console.error(response.error.message);
        throw Error(response.error.code);
    } else {
        return response.result;
    }
}

module.exports = service;
