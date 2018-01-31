let Pushover = require('node-pushover'),
    config = require('../config/pushover');

var push = new Pushover(config.credentials);

module.exports = {
    send: (title, message) => {
        if (config.credentials.token)
            push.send(title, message);
    }
};
