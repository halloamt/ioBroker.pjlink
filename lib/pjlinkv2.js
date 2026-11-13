/**
 *
 *      library for iobroker pjlink Adapter
 *
 *      Modul :     pjlinkv2
 *      Version:    0.1
 *      Stand:      07.09.2025
 *
 *      Copyright (c) 2025, Bannsaenger <bannsaenger@gmx.de>
 *
 *      MIT License
 *
 */
'use strict';
const net = require('node:net');
const crypto = require('node:crypto');

/**
 * connectionState:
 * NEW:                 When no connection attempt was made till now
 * CONNECTING:          When the connection attempt is running
 * CONNECTED:           When everytiung runs fine. Also when no authentication is needed, then the connection is established without any further doing
 * AUTH1:               When the old authentication is supported
 * AUTH2:               When we have to use the new authentication process
 * READY:               After all authentication is done and we can send data
 * CLOSED:              When something closed the client
 * FAILED:              When a connection attempt failed
 * RECONNECT_WAITING:   When the connection has failed and the reconnect time is running
 */

/** Class encapsuling the basic PJLink communication */
module.exports = class pjlink {
    /**
     * Creates a new PJLink communication instance
     *
     * @param {object} options - Options for the whole instance
     */
    constructor(options = {}) {
        // Parse all options and set defaults
        this.host = options.host || '127.0.0.1';    // address or name of the projector
        this.port = options.port || 4352;           // tcp port of the projector
        this.class = options.class || 1;            // for now only class 1 is supported
        this.password = options.password || null,   // null, if no password/security needed
        this.socketTimeout = options.socketTimeout|| 800; // time to wait for connection

        // Init Logger, if object it should be a iobroker class log
        if (typeof options.logger === 'object') {
            this.log = options.logger;
        } else {
            this.log = new scriptLogger();
        }

        if (this.class != 1) {
            this.log.error(`Only class 1 supported for now`);
            this.class = 1;
        }

        this.cmdWaiting = false;
        this.connectionState = 'NEW';

        // error deduplication: every error is logged only once when it newly occurs.
        // Repetitions are logged as debug until the connection is working again.
        this.lastConnectionError = null; // key (e.g. 'ECONNREFUSED') of the actual connection error
        this.loggedErrors = new Set(); // keys of errors logged by the errorHandler

        this.log.info(`projector instance for host: ${this.host} created`);
    }
    /**********************************************************************************************
     * Methods to handle errors and log the messages
     **********************************************************************************************/
    /* #region log and Error handling */
    /**
     * Called on error situations and from catch blocks
     *
     * @param {any} err the error to log
     * @param {string} module the module in which the error occured
     */
    errorHandler(err, module = '') {
        const message = err && err.message ? err.message : String(err);
        const key = `${module}|${message}`;
        if (this.loggedErrors.has(key)) {
            this.log.debug(`PJLink(module) error in method: [${module}] error: ${message} (repeated)`);
            return;
        }
        this.loggedErrors.add(key);
        this.log.error(
            `PJLink(module) error in method: [${module}] error: ${message}, stack: ${err && err.stack}. Repetitions are logged as debug only`,
        );
    }

    /**
     * Log a connection error only once when it newly occurs or changes.
     * Repetitions of the same error are logged as debug.
     *
     * @param {string} key a short key to identify the error (e.g. the error code)
     * @param {string} message the message to log
     */
    logConnectionError(key, message) {
        if (this.lastConnectionError === key) {
            this.log.debug(`${message} (repeated)`);
            return;
        }
        this.lastConnectionError = key;
        this.log.error(`${message}. Repetitions are logged as debug until the connection is working again`);
    }

    /**
     * Called when the connection works again. Logs the recovery once and resets the error memory.
     */
    clearConnectionError() {
        if (this.lastConnectionError !== null || this.loggedErrors.size > 0) {
            this.log.info(`Projector: connection to ${this.host} is working again`);
        }
        this.lastConnectionError = null;
        this.loggedErrors.clear();
    }
    /* #endregion */

     /**********************************************************************************************
     * Methods for connection handling
     **********************************************************************************************/
    /* #region connection handling */
    /**
     * connect to the projector, prepare all connection related parameters
     */
    connect() {
        try {
            // clean up a previous socket (e.g. still hanging in a connection attempt),
            // otherwise every reconnect cycle would leave an open socket behind
            if (this.connection) {
                const oldConnection = this.connection;
                oldConnection.removeAllListeners();
                oldConnection.on('error', () => {}); // swallow late errors of the old socket
                oldConnection.destroy();
                this.connection = undefined;
            }
            this.cmdWaiting = true;
            this.connectionState = 'CONNECTING';
            this.connection = net.connect({port: this.port, host: this.host}, this.onClientConnect.bind(this));

            //callbacks
            this.dataCB = this.onClientData.bind(this);
            this.errorCB = this.onClientError.bind(this);
            this.closeCB = this.onClientClose.bind(this);
            this.timeoutCB = this.onClientTimeout.bind(this);

            this.connection.on('data', this.dataCB);
            this.connection.on('error', this.errorCB);
            this.connection.on('close', this.closeCB);
            this.connection.on('timeout', this.timeoutCB);
            this.connection.setTimeout(this.socketTimeout);
            this.connection.setNoDelay(true);
        } catch (err) {
            this.errorHandler(err, 'connect');
        }
    }
    /* #endregion */

    /**********************************************************************************************
     * Methods related to TCP Client handling
     **********************************************************************************************/
    /* #region Client handling */
    // #################################### NET ####################################
    /**
     * is called when client has connected
     */
    onClientConnect() {
        try {
        	this.cmdWaiting = false;
            this.connectionState = 'CONNECTED';
            this.log.debug(`Projector: with ${this.host} connected`);
            this.clearConnectionError();
            // now try to authenticate
        } catch (err) {
            this.errorHandler(err, 'onClientConnect');
        }
    }

    /**
     * is called when the client connection runs into a timeout on connection attempt
     */
    onClientTimeout() {
        try {
            if (this.connectionState === 'CONNECTING') {
                // timeout during the connection attempt -> projector not reachable
                this.connectionState = 'FAILED';
                this.logConnectionError('TIMEOUT', `Projector: connection to ${this.host} has timed out`);
                this.connection?.destroy();
                this.connection = undefined;
            } else {
                this.log.debug(`Projector: with ${this.host} connection has timed out`);
            }
        } catch (err) {
            this.errorHandler(err, 'onClientTimeout');
        }
    }

    /**
     * is called when specified client connection has an error
     *
     * @param {Error} err the error occured
     }
     */
    onClientError(err) {
        try {
            this.logConnectionError(
                err && err.code ? err.code : String(err),
                `TCP Client: with ${this.host} has an error: ${err.toString()}`,
            );
            //const device = this.devices.find(item => item.ipAddress === ipAddress);
            this.connectionState = 'FAILED';
            this.connection?.destroy();
            this.connection = undefined;
        } catch (err) {
            this.errorHandler(err, 'onClientError');
        }
    }

    /**
     * is called when specified client connection is closed
     */
    onClientClose() {
        try {
            if (this.lastConnectionError !== null) {
                // closing after an error is the normal consequence of that error, no need to log it again
                this.log.debug(`TCP Client: to ${this.host} is closed`);
            } else {
                this.log.info(`TCP Client: to ${this.host} is closed`);
            }
            this.connectionState = 'CLOSED';
            this.connection?.destroy();
            this.connection = undefined;
        } catch (err) {
            this.errorHandler(err, 'onClientClose');
        }
    }

    /**
     * called if client is ready to send new data
     */
    onClientContinue() {
        try {
            this.log.debug(`TCP Client: to ${this.host} stream can continue`);
        } catch (err) {
            this.errorHandler(err, 'onClientContinue');
        }
    }

    /**
     * called if client receives data
     *
     * @param {Buffer} data the data received
     */
    onClientData(data) {
        try {
            this.log.debug(`TCP Client: to ${this.host} got data <${data.toString().replace(/\r/g, '')}>`);
        } catch (err) {
            this.errorHandler(err, 'onClientData');
        }
    }
    /* #endregion */

}

/**
 * Helper Class to write logging like the real adapter programming
 */
class scriptLogger {
    constructor() {}
    silly(message = '') {
        console.log(`${message}`, 'silly');
    }
    trace(message = '') {
        console.log(`${message}`, 'silly');
    }
    debug(message = '') {
        console.log(`${message}`, 'debug');
    }
    info(message = '') {
        console.log(`${message}`, 'info');
    }
    warn(message = '') {
        console.log(`${message}`, 'warn');
    }
    error(message = '') {
        console.log(`${message}`, 'error');
    }
    fatal(message = '') {
        console.log(`${message}`, 'error');
    }
}
