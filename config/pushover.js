module.exports = {
    credentials: {
        token: (process.env.PUSHOVER_TOKEN) ? process.env.PUSHOVER_TOKEN : "",
        user: (process.env.PUSHOVER_USER) ? process.env.PUSHOVER_USER : ""
    }
};
