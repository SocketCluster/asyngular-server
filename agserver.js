const AGServerSocket = require('./agserversocket');
const AuthEngine = require('sc-auth').AuthEngine;
const formatter = require('sc-formatter');
const base64id = require('base64id');
const async = require('async');
const url = require('url');
const crypto = require('crypto');
const uuid = require('uuid');
const SCSimpleBroker = require('sc-simple-broker').SCSimpleBroker;
const AsyncStreamEmitter = require('async-stream-emitter');

const scErrors = require('sc-errors');
const AuthTokenExpiredError = scErrors.AuthTokenExpiredError;
const AuthTokenInvalidError = scErrors.AuthTokenInvalidError;
const AuthTokenNotBeforeError = scErrors.AuthTokenNotBeforeError;
const AuthTokenError = scErrors.AuthTokenError;
const SilentMiddlewareBlockedError = scErrors.SilentMiddlewareBlockedError;
const InvalidArgumentsError = scErrors.InvalidArgumentsError;
const InvalidOptionsError = scErrors.InvalidOptionsError;
const InvalidActionError = scErrors.InvalidActionError;
const BrokerError = scErrors.BrokerError;
const ServerProtocolError = scErrors.ServerProtocolError;

function AGServer(options) {
  AsyncStreamEmitter.call(this);

  let opts = {
    brokerEngine: new SCSimpleBroker(),
    wsEngine: 'ws',
    wsEngineServerOptions: {},
    maxPayload: null,
    allowClientPublish: true,
    ackTimeout: 10000,
    handshakeTimeout: 10000,
    pingTimeout: 20000,
    pingTimeoutDisabled: false,
    pingInterval: 8000,
    origins: '*:*',
    appName: uuid.v4(),
    path: '/socketcluster/',
    authDefaultExpiry: 86400,
    authSignAsync: false,
    authVerifyAsync: true,
    pubSubBatchDuration: null,
    middlewareEmitWarnings: true
  };

  this.options = Object.assign(opts, options);

  this._middleware = {};
  this._middleware[this.MIDDLEWARE_HANDSHAKE_WS] = [];
  this._middleware[this.MIDDLEWARE_HANDSHAKE_AG] = [];

  this.origins = opts.origins;
  this._allowAllOrigins = this.origins.indexOf('*:*') !== -1;

  this.ackTimeout = opts.ackTimeout;
  this.handshakeTimeout = opts.handshakeTimeout;
  this.pingInterval = opts.pingInterval;
  this.pingTimeout = opts.pingTimeout;
  this.pingTimeoutDisabled = opts.pingTimeoutDisabled;
  this.allowClientPublish = opts.allowClientPublish;
  this.perMessageDeflate = opts.perMessageDeflate;
  this.httpServer = opts.httpServer;
  this.socketChannelLimit = opts.socketChannelLimit;

  this.brokerEngine = opts.brokerEngine;
  this.appName = opts.appName || '';
  this.middlewareEmitWarnings = opts.middlewareEmitWarnings;

  // Make sure there is always a leading and a trailing slash in the WS path.
  this._path = opts.path.replace(/\/?$/, '/').replace(/^\/?/, '/');

  if (this.brokerEngine.isReady) {
    this.isReady = true;
    this.emit('ready', {});
  } else {
    this.isReady = false;
    (async () => {
      await this.brokerEngine.listener('ready').once();
      this.isReady = true;
      this.emit('ready', {});
    })();
  }

  let wsEngine = typeof opts.wsEngine === 'string' ? require(opts.wsEngine) : opts.wsEngine;
  if (!wsEngine || !wsEngine.Server) {
    throw new InvalidOptionsError(
      'The wsEngine option must be a path or module name which points ' +
      'to a valid WebSocket engine module with a compatible interface'
    );
  }
  let WSServer = wsEngine.Server;

  if (opts.authPrivateKey != null || opts.authPublicKey != null) {
    if (opts.authPrivateKey == null) {
      throw new InvalidOptionsError(
        'The authPrivateKey option must be specified if authPublicKey is specified'
      );
    } else if (opts.authPublicKey == null) {
      throw new InvalidOptionsError(
        'The authPublicKey option must be specified if authPrivateKey is specified'
      );
    }
    this.signatureKey = opts.authPrivateKey;
    this.verificationKey = opts.authPublicKey;
  } else {
    if (opts.authKey == null) {
      opts.authKey = crypto.randomBytes(32).toString('hex');
    }
    this.signatureKey = opts.authKey;
    this.verificationKey = opts.authKey;
  }

  this.authVerifyAsync = opts.authVerifyAsync;
  this.authSignAsync = opts.authSignAsync;

  this.defaultVerificationOptions = {
    async: this.authVerifyAsync
  };
  if (opts.authVerifyAlgorithms != null) {
    this.defaultVerificationOptions.algorithms = opts.authVerifyAlgorithms;
  } else if (opts.authAlgorithm != null) {
    this.defaultVerificationOptions.algorithms = [opts.authAlgorithm];
  }

  this.defaultSignatureOptions = {
    expiresIn: opts.authDefaultExpiry,
    async: this.authSignAsync
  };
  if (opts.authAlgorithm != null) {
    this.defaultSignatureOptions.algorithm = opts.authAlgorithm;
  }

  if (opts.authEngine) {
    this.auth = opts.authEngine;
  } else {
    // Default authentication engine
    this.auth = new AuthEngine();
  }

  if (opts.codecEngine) {
    this.codec = opts.codecEngine;
  } else {
    // Default codec engine
    this.codec = formatter;
  }

  this.clients = {};
  this.clientsCount = 0;

  this.pendingClients = {};
  this.pendingClientsCount = 0;

  this.exchange = this.brokerEngine.exchange();

  let wsServerOptions = opts.wsEngineServerOptions || {};
  wsServerOptions.server = this.httpServer;
  wsServerOptions.verifyClient = this.verifyHandshake.bind(this);

  if (wsServerOptions.path == null && this._path != null) {
    wsServerOptions.path = this._path;
  }
  if (wsServerOptions.perMessageDeflate == null && this.perMessageDeflate != null) {
    wsServerOptions.perMessageDeflate = this.perMessageDeflate;
  }
  if (wsServerOptions.handleProtocols == null && opts.handleProtocols != null) {
    wsServerOptions.handleProtocols = opts.handleProtocols;
  }
  if (wsServerOptions.maxPayload == null && opts.maxPayload != null) {
    wsServerOptions.maxPayload = opts.maxPayload;
  }
  if (wsServerOptions.clientTracking == null) {
    wsServerOptions.clientTracking = false;
  }

  this.wsServer = new WSServer(wsServerOptions);

  this.wsServer.on('error', this._handleServerError.bind(this));
  this.wsServer.on('connection', this._handleSocketConnection.bind(this));
}

AGServer.prototype = Object.create(AsyncStreamEmitter.prototype);

AGServer.prototype.MIDDLEWARE_HANDSHAKE_WS = AGServer.MIDDLEWARE_HANDSHAKE_WS = 'handshakeWS';
AGServer.prototype.MIDDLEWARE_HANDSHAKE_AG = AGServer.MIDDLEWARE_HANDSHAKE_AG = 'handshakeAG';

AGServer.prototype.setAuthEngine = function (authEngine) {
  this.auth = authEngine;
};

AGServer.prototype.setCodecEngine = function (codecEngine) {
  this.codec = codecEngine;
};

AGServer.prototype.emitError = function (error) {
  this.emit('error', {error});
};

AGServer.prototype.emitWarning = function (warning) {
  this.emit('warning', {warning});
};

AGServer.prototype._handleServerError = function (error) {
  if (typeof error === 'string') {
    error = new ServerProtocolError(error);
  }
  this.emitError(error);
};

AGServer.prototype._handleSocketErrors = async function (socket) {
  // A socket error will show up as a warning on the server.
  for await (let event of socket.listener('error')) {
    this.emitWarning(event.error);
  }
};

AGServer.prototype._handleHandshakeTimeout = function (scSocket) {
  scSocket.disconnect(4005);
};

AGServer.prototype._subscribeSocket = async function (socket, channelOptions) {
  if (!channelOptions) {
    throw new InvalidActionError(`Socket ${socket.id} provided a malformated channel payload`);
  }

  if (this.socketChannelLimit && socket.channelSubscriptionsCount >= this.socketChannelLimit) {
    throw new InvalidActionError(
      `Socket ${socket.id} tried to exceed the channel subscription limit of ${this.socketChannelLimit}`
    );
  }

  let channelName = channelOptions.channel;

  if (typeof channelName !== 'string') {
    throw new InvalidActionError(`Socket ${socket.id} provided an invalid channel name`);
  }

  if (socket.channelSubscriptionsCount == null) {
    socket.channelSubscriptionsCount = 0;
  }
  if (socket.channelSubscriptions[channelName] == null) {
    socket.channelSubscriptions[channelName] = true;
    socket.channelSubscriptionsCount++;
  }

  try {
    await this.brokerEngine.subscribeSocket(socket, channelName);
  } catch (err) {
    delete socket.channelSubscriptions[channelName];
    socket.channelSubscriptionsCount--;
    throw err;
  }
  socket.emit('subscribe', {
    channel: channelName,
    subscribeOptions: channelOptions
  });
  this.emit('subscription', {
    socket,
    channel: channelName,
    subscribeOptions: channelOptions
  });
};

AGServer.prototype._unsubscribeSocketFromAllChannels = function (socket) {
  Object.keys(socket.channelSubscriptions).forEach((channelName) => {
    this._unsubscribeSocket(socket, channelName);
  });
};

AGServer.prototype._unsubscribeSocket = function (socket, channel) {
  if (typeof channel !== 'string') {
    throw new InvalidActionError(
      `Socket ${socket.id} tried to unsubscribe from an invalid channel name`
    );
  }
  if (!socket.channelSubscriptions[channel]) {
    throw new InvalidActionError(
      `Socket ${socket.id} tried to unsubscribe from a channel which it is not subscribed to`
    );
  }

  delete socket.channelSubscriptions[channel];
  if (socket.channelSubscriptionsCount != null) {
    socket.channelSubscriptionsCount--;
  }

  this.brokerEngine.unsubscribeSocket(socket, channel);

  socket.emit('unsubscribe', {channel});
  this.emit('unsubscription', {socket, channel});
};

AGServer.prototype._handleSocketConnection = function (wsSocket, upgradeReq) {
  if (!wsSocket.upgradeReq) {
    // Normalize ws modules to match.
    wsSocket.upgradeReq = upgradeReq;
  }

  let id = this.generateId();

  let scSocket = new AGServerSocket(id, this, wsSocket);
  scSocket.exchange = this.exchange;

  this._handleSocketErrors(scSocket);

  this.pendingClients[id] = scSocket;
  this.pendingClientsCount++;

  let handleSocketAuthenticate = async () => {
    for await (let rpc of scSocket.procedure('#authenticate')) {
      let signedAuthToken = rpc.data;

      scSocket.processAuthToken(signedAuthToken, (err, isBadToken, oldAuthState) => {
        if (err) {
          if (isBadToken) {
            scSocket.deauthenticate();
          }
        } else {
          scSocket.triggerAuthenticationEvents(oldAuthState);
        }
        if (err && isBadToken) {
          rpc.error(err);
        } else {
          let authStatus = {
            isAuthenticated: !!scSocket.authToken,
            authError: scErrors.dehydrateError(err)
          };
          rpc.end(authStatus);
        }
      });
    }
  };
  handleSocketAuthenticate();

  let handleSocketRemoveAuthToken = async () => {
    for await (let data of scSocket.receiver('#removeAuthToken')) {
      scSocket.deauthenticateSelf();
    }
  };
  handleSocketRemoveAuthToken();

  let handleSocketSubscribe = async () => {
    for await (let rpc of scSocket.procedure('#subscribe')) {
      let channelOptions = rpc.data;

      if (!channelOptions) {
        channelOptions = {};
      } else if (typeof channelOptions === 'string') {
        channelOptions = {
          channel: channelOptions
        };
      }

      (async () => {
        if (scSocket.state === scSocket.OPEN) {
          try {
            await this._subscribeSocket(scSocket, channelOptions);
          } catch (err) {
            let error = new BrokerError(`Failed to subscribe socket to the ${channelOptions.channel} channel - ${err}`);
            rpc.error(error);
            scSocket.emitError(error);
            return;
          }
          if (channelOptions.batch) {
            rpc.end(undefined, {batch: true});
            return;
          }
          rpc.end();
          return;
        }
        // This is an invalid state; it means the client tried to subscribe before
        // having completed the handshake.
        let error = new InvalidActionError('Cannot subscribe socket to a channel before it has completed the handshake');
        rpc.error(error);
        this.emitWarning(error);
      })();
    }
  };
  handleSocketSubscribe();

  let handleSocketUnsubscribe = async () => {
    for await (let rpc of scSocket.procedure('#unsubscribe')) {
      let channel = rpc.data;
      let error;
      try {
        this._unsubscribeSocket(scSocket, channel);
      } catch (err) {
        error = new BrokerError(
          `Failed to unsubscribe socket from the ${channel} channel - ${err}`
        );
      }
      if (error) {
        rpc.error(error);
        scSocket.emitError(error);
      } else {
        rpc.end();
      }
    }
  };
  handleSocketUnsubscribe();

  let cleanupSocket = (type, code, reason) => {
    clearTimeout(scSocket._handshakeTimeoutRef);

    scSocket.closeProcedure('#handshake');
    scSocket.closeProcedure('#authenticate');
    scSocket.closeProcedure('#subscribe');
    scSocket.closeProcedure('#unsubscribe');
    scSocket.closeReceiver('#removeAuthToken');
    scSocket.closeListener('authenticate');
    scSocket.closeListener('authStateChange');
    scSocket.closeListener('deauthenticate');

    let isClientFullyConnected = !!this.clients[id];

    if (isClientFullyConnected) {
      delete this.clients[id];
      this.clientsCount--;
    }

    let isClientPending = !!this.pendingClients[id];
    if (isClientPending) {
      delete this.pendingClients[id];
      this.pendingClientsCount--;
    }

    if (type === 'disconnect') {
      this.emit('disconnection', {
        socket: scSocket,
        code,
        reason
      });
    } else if (type === 'abort') {
      this.emit('connectionAbort', {
        socket: scSocket,
        code,
        reason
      });
    }
    this.emit('closure', {
      socket: scSocket,
      code,
      reason
    });

    this._unsubscribeSocketFromAllChannels(scSocket);
  };

  let handleSocketDisconnect = async () => {
    let event = await scSocket.listener('disconnect').once();
    cleanupSocket('disconnect', event.code, event.data);
  };
  handleSocketDisconnect();

  let handleSocketAbort = async () => {
    let event = await scSocket.listener('connectAbort').once();
    cleanupSocket('abort', event.code, event.data);
  };
  handleSocketAbort();

  scSocket._handshakeTimeoutRef = setTimeout(this._handleHandshakeTimeout.bind(this, scSocket), this.handshakeTimeout);

  let handleSocketHandshake = async () => {
    for await (let rpc of scSocket.procedure('#handshake')) {
      let data = rpc.data || {};
      let signedAuthToken = data.authToken || null;
      clearTimeout(scSocket._handshakeTimeoutRef);

      this._passThroughHandshakeAGMiddleware({
        socket: scSocket
      }, (err, statusCode) => {
        if (err) {
          if (err.statusCode == null) {
            err.statusCode = statusCode;
          }
          rpc.error(err);
          scSocket.disconnect(err.statusCode);
          return;
        }
        scSocket.processAuthToken(signedAuthToken, (err, isBadToken, oldAuthState) => {
          if (scSocket.state === scSocket.CLOSED) {
            return;
          }

          let clientSocketStatus = {
            id: scSocket.id,
            pingTimeout: this.pingTimeout
          };
          let serverSocketStatus = {
            id: scSocket.id,
            pingTimeout: this.pingTimeout
          };

          if (err) {
            if (signedAuthToken != null) {
              // Because the token is optional as part of the handshake, we don't count
              // it as an error if the token wasn't provided.
              clientSocketStatus.authError = scErrors.dehydrateError(err);
              serverSocketStatus.authError = err;

              if (isBadToken) {
                scSocket.deauthenticate();
              }
            }
          }
          clientSocketStatus.isAuthenticated = !!scSocket.authToken;
          serverSocketStatus.isAuthenticated = clientSocketStatus.isAuthenticated;

          if (this.pendingClients[id]) {
            delete this.pendingClients[id];
            this.pendingClientsCount--;
          }
          this.clients[id] = scSocket;
          this.clientsCount++;

          scSocket.state = scSocket.OPEN;

          if (clientSocketStatus.isAuthenticated) {
            // Needs to be executed after the connection event to allow
            // consumers to be setup from inside the connection loop.
            (async () => {
              await this.listener('connection').once();
              scSocket.triggerAuthenticationEvents(oldAuthState);
            })();
          }

          scSocket.emit('connect', serverSocketStatus);
          this.emit('connection', {socket: scSocket, ...serverSocketStatus});

          // Treat authentication failure as a 'soft' error
          rpc.end(clientSocketStatus);
        });
      });
    }
  };
  handleSocketHandshake();

  // Emit event to signal that a socket handshake has been initiated.
  this.emit('handshake', {socket: scSocket});
};

AGServer.prototype.close = function () {
  this.isReady = false;
  return new Promise((resolve, reject) => {
    this.wsServer.close((err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });
};

AGServer.prototype.getPath = function () {
  return this._path;
};

AGServer.prototype.generateId = function () {
  return base64id.generateId();
};

AGServer.prototype.addMiddleware = function (type, middleware) {
  if (!this._middleware[type]) {
    throw new InvalidArgumentsError(`Middleware type "${type}" is not supported on AGServer instance`);
    // Read more: https://socketcluster.io/#!/docs/middleware-and-authorization
  }
  this._middleware[type].push(middleware);
};

AGServer.prototype.removeMiddleware = function (type, middleware) {
  let middlewareFunctions = this._middleware[type];

  this._middleware[type] = middlewareFunctions.filter((fn) => {
    return fn !== middleware;
  });
};

AGServer.prototype.verifyHandshake = function (info, callback) {
  let req = info.req;
  let origin = info.origin;
  if (origin === 'null' || origin == null) {
    origin = '*';
  }
  let ok = false;

  if (this._allowAllOrigins) {
    ok = true;
  } else {
    try {
      let parts = url.parse(origin);
      parts.port = parts.port || 80;
      ok = ~this.origins.indexOf(parts.hostname + ':' + parts.port) ||
        ~this.origins.indexOf(parts.hostname + ':*') ||
        ~this.origins.indexOf('*:' + parts.port);
    } catch (e) {}
  }

  if (ok) {
    let handshakeMiddleware = this._middleware[this.MIDDLEWARE_HANDSHAKE_WS];
    if (handshakeMiddleware.length) {
      let callbackInvoked = false;
      async.applyEachSeries(handshakeMiddleware, req, (err) => {
        if (callbackInvoked) {
          this.emitWarning(
            new InvalidActionError(
              `Callback for ${this.MIDDLEWARE_HANDSHAKE_WS} middleware was already invoked`
            )
          );
        } else {
          callbackInvoked = true;
          if (err) {
            if (err === true || err.silent) {
              err = new SilentMiddlewareBlockedError(
                `Action was silently blocked by ${this.MIDDLEWARE_HANDSHAKE_WS} middleware`,
                this.MIDDLEWARE_HANDSHAKE_WS
              );
            } else if (this.middlewareEmitWarnings) {
              this.emitWarning(err);
            }
            callback(false, 401, typeof err === 'string' ? err : err.message);
          } else {
            callback(true);
          }
        }
      });
    } else {
      callback(true);
    }
  } else {
    let err = new ServerProtocolError(
      `Failed to authorize socket handshake - Invalid origin: ${origin}`
    );
    this.emitWarning(err);
    callback(false, 403, err.message);
  }
};

AGServer.prototype._passThroughHandshakeAGMiddleware = function (options, callback) {
  let callbackInvoked = false;

  let request = {
    socket: options.socket
  };

  async.applyEachSeries(this._middleware[this.MIDDLEWARE_HANDSHAKE_AG], request,
    (err, results) => {
      if (callbackInvoked) {
        this.emitWarning(
          new InvalidActionError(
            `Callback for ${this.MIDDLEWARE_HANDSHAKE_AG} middleware was already invoked`
          )
        );
      } else {
        callbackInvoked = true;
        let statusCode;
        if (results.length) {
          statusCode = results[results.length - 1] || 4008;
        } else {
          statusCode = 4008;
        }
        if (err) {
          if (err.statusCode != null) {
            statusCode = err.statusCode;
          }
          if (err === true || err.silent) {
            err = new SilentMiddlewareBlockedError(
              `Action was silently blocked by ${this.MIDDLEWARE_HANDSHAKE_AG} middleware`,
              this.MIDDLEWARE_HANDSHAKE_AG
            );
          } else if (this.middlewareEmitWarnings) {
            this.emitWarning(err);
          }
        }
        callback(err, statusCode);
      }
    }
  );
};

module.exports = AGServer;
