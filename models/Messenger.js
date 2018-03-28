let Pushover = require('node-pushover'),
    config = require('../config/pushover');

var push = (config.credentials.token)?new Pushover(config.credentials):null;

module.exports = {
    send: (title, message) => {
        if (config.credentials.token)
            push.send(title, message);
    }
};
