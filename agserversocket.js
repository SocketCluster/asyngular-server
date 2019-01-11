const cloneDeep = require('lodash.clonedeep');
const StreamDemux = require('stream-demux');
const AsyncStreamEmitter = require('async-stream-emitter');
// const Response = require('./response'); // TODO 2
const Action = require('./action');
const Request = require('./request');

const scErrors = require('sc-errors');
const InvalidArgumentsError = scErrors.InvalidArgumentsError;
const SocketProtocolError = scErrors.SocketProtocolError;
const TimeoutError = scErrors.TimeoutError;
const InvalidActionError = scErrors.InvalidActionError;
const AuthError = scErrors.AuthError;
const SilentMiddlewareBlockedError = scErrors.SilentMiddlewareBlockedError;

function AGServerSocket(id, server, socket) {
  AsyncStreamEmitter.call(this);

  this._autoAckRPCs = {
    '#publish': 1
  };

  this.id = id;
  this.server = server;
  this.socket = socket;
  this.state = this.CONNECTING;
  this.authState = this.UNAUTHENTICATED;

  this._receiverDemux = new StreamDemux();
  this._procedureDemux = new StreamDemux();

  this.request = this.socket.upgradeReq || {};

  if (this.request.connection) {
    this.remoteAddress = this.request.connection.remoteAddress;
    this.remoteFamily = this.request.connection.remoteFamily;
    this.remotePort = this.request.connection.remotePort;
  } else {
    this.remoteAddress = this.request.remoteAddress;
    this.remoteFamily = this.request.remoteFamily;
    this.remotePort = this.request.remotePort;
  }
  if (this.request.forwardedForAddress) {
    this.forwardedForAddress = this.request.forwardedForAddress;
  }

  this._cid = 1;
  this._callbackMap = {};
  this._batchSendList = [];

  this.channelSubscriptions = {};
  this.channelSubscriptionsCount = 0;

  this._middlewareDemux = new StreamDemux();
  this._middlewareInboundStream = this._middlewareDemux.stream('inbound');
  this._middlewareOutboundStream = this._middlewareDemux.stream('outbound');

  this.socket.on('error', (err) => {
    this.emitError(err);
  });

  this.socket.on('close', (code, data) => {
    this._onClose(code, data);
  });

  if (!this.server.pingTimeoutDisabled) {
    this._pingIntervalTicker = setInterval(this._sendPing.bind(this), this.server.pingInterval);
  }
  this._resetPongTimeout();

  // Receive incoming raw messages
  this.socket.on('message', (message, flags) => {
    this._resetPongTimeout();

    this.emit('message', {message});

    let packet;
    try {
      packet = this.decode(message);
    } catch (err) {
      if (err.name === 'Error') {
        err.name = 'InvalidMessageError';
      }
      this.emitError(err);
      return;
    }

    // If pong
    if (packet === '#2') {
      let token = this.getAuthToken();
      if (this.isAuthTokenExpired(token)) {
        this.deauthenticate();
      }
    } else {
      if (Array.isArray(packet)) {
        let len = packet.length;
        for (let i = 0; i < len; i++) {
          this._processInboundPacket(packet[i], message);
        }
      } else {
        this._processInboundPacket(packet, message);
      }
    }
  });
}

AGServerSocket.prototype = Object.create(AsyncStreamEmitter.prototype);

AGServerSocket.prototype.MIDDLEWARE_TRANSMIT = AGServerSocket.MIDDLEWARE_TRANSMIT = 'transmit';
AGServerSocket.prototype.MIDDLEWARE_INVOKE = AGServerSocket.MIDDLEWARE_INVOKE = 'invoke';
AGServerSocket.prototype.MIDDLEWARE_SUBSCRIBE = AGServerSocket.MIDDLEWARE_SUBSCRIBE = 'subscribe';
AGServerSocket.prototype.MIDDLEWARE_PUBLISH_IN = AGServerSocket.MIDDLEWARE_PUBLISH_IN = 'publishIn';
AGServerSocket.prototype.MIDDLEWARE_PUBLISH_OUT = AGServerSocket.MIDDLEWARE_PUBLISH_OUT = 'publishOut';
AGServerSocket.prototype.MIDDLEWARE_AUTHENTICATE = AGServerSocket.MIDDLEWARE_AUTHENTICATE = 'authenticate';

AGServerSocket.CONNECTING = AGServerSocket.prototype.CONNECTING = 'connecting';
AGServerSocket.OPEN = AGServerSocket.prototype.OPEN = 'open';
AGServerSocket.CLOSED = AGServerSocket.prototype.CLOSED = 'closed';

AGServerSocket.AUTHENTICATED = AGServerSocket.prototype.AUTHENTICATED = 'authenticated';
AGServerSocket.UNAUTHENTICATED = AGServerSocket.prototype.UNAUTHENTICATED = 'unauthenticated';

AGServerSocket.ignoreStatuses = scErrors.socketProtocolIgnoreStatuses;
AGServerSocket.errorStatuses = scErrors.socketProtocolErrorStatuses;

AGServerSocket.prototype.middleware = function (type) {
  // TODO 2: type can be inbound or outbound; throw error if neither.
  // TODO 2: use _middlewareInboundStream and _middlewareOutboundStream instead.
  return this._middlewareDemux.stream(type);
};

AGServerSocket.prototype.receiver = function (receiverName) {
  return this._receiverDemux.stream(receiverName);
};

AGServerSocket.prototype.closeReceiver = function (receiverName) {
  this._receiverDemux.close(receiverName);
};

AGServerSocket.prototype.procedure = function (procedureName) {
  return this._procedureDemux.stream(procedureName);
};

AGServerSocket.prototype.closeProcedure = function (procedureName) {
  this._procedureDemux.close(procedureName);
};

AGServerSocket.prototype._sendPing = function () {
  if (this.state !== this.CLOSED) {
    this.sendObject('#1');
  }
};

AGServerSocket.prototype._processMiddlewareAction = async function (middlewareStream, action) {
  middlewareStream.write(action);

  let newData;
  try {
    newData = await action.promise;
  } catch (error) {
    let clientError;
    if (error.silent) {
      clientError = new SilentMiddlewareBlockedError(
        `Action was blocked by ${action.name} middleware`,
        action.name
      );
    } else {
      clientError = error;
    }
    if (this.server.middlewareEmitWarnings) { // TODO 2: Rename middlewareEmitWarnings to middlewareEmitFailures
      this.emitError(error);
    }
    throw clientError;
  }

  if (newData === undefined) {
    newData = action.data;
  }

  return newData;
};

// TODO 2: AUTHORIZATION MIDDLEWARE check badToken logic

AGServerSocket.prototype._processInboundPacket = async function (packet, message) {
  if (packet && packet.event != null) {
    let eventName = packet.event;
    let actionName;
    let isRPC = packet.cid != null;

    if (this._isReservedRemoteEvent(eventName)) {
      if (eventName === '#publish') {
        actionName = this.MIDDLEWARE_PUBLISH_IN;
      } else if (eventName === '#subscribe') {
        actionName = this.MIDDLEWARE_SUBSCRIBE;
      } else {
        this.emitError(
          new InvalidActionError(
            `The reserved inbound action ${eventName} is not supported`
          )
        );
        return;
      }
    } else {
      if (isRPC) {
        actionName = this.MIDDLEWARE_INVOKE;
      } else {
        actionName = this.MIDDLEWARE_TRANSMIT;
      }
    }

    let action = new Action(actionName, packet.data);

    let tokenExpiredError = this._processAuthTokenExpiry();
    if (tokenExpiredError) {
      action.authTokenExpiredError = tokenExpiredError;
    }

    let newData;

    if (isRPC) {
      try {
        newData = await this._processMiddlewareAction(this._middlewareInboundStream, action);
      } catch (error) {
        let req = new Request(this, packet.cid);
        req.error(clientError);

        return;
      }

      let req = new Request(this, packet.cid, newData);

      if (this._autoAckRPCs[eventName]) {
        if (newData === undefined) {
          req.end();
        } else {
          req.end(newData);
        }
      } else {
        this._procedureDemux.write(eventName, req);
      }

      return;
    } else {
      try {
        newData = await this._processMiddlewareAction(this._middlewareInboundStream, action);
      } catch (error) {

        return;
      }
    }

    this._receiverDemux.write(eventName, newData);

  } else if (packet && packet.rid != null) {
    // If incoming message is a response to a previously sent message
    let ret = this._callbackMap[packet.rid];
    if (ret) {
      clearTimeout(ret.timeout);
      delete this._callbackMap[packet.rid];
      let rehydratedError = scErrors.hydrateError(packet.error);
      ret.callback(rehydratedError, packet.data);
    }
  } else {
    // The last remaining case is to treat the message as raw
    this.emit('raw', {message});
  }
};

AGServerSocket.prototype._processOutboundPacket = async function (eventName, eventData, options) { // TODO 3
  let actionName = this.MIDDLEWARE_PUBLISH_OUT;
  let action = new Action(actionName, eventData);

  this._middlewareOutboundStream.write(action);

  let newData = await action.promise;
  try {
  } catch (error) {
    if (this.server.middlewareEmitWarnings) { // TODO 2: Rename middlewareEmitWarnings to middlewareEmitFailures
      this.emitError(error);
    }
    throw error;
  }
  if (newData === undefined) {
    newData = eventData;
  }
  return newData;
};

AGServerSocket.prototype._resetPongTimeout = function () {
  if (this.server.pingTimeoutDisabled) {
    return;
  }
  clearTimeout(this._pingTimeoutTicker);
  this._pingTimeoutTicker = setTimeout(() => {
    this._onClose(4001);
    this.socket.close(4001);
  }, this.server.pingTimeout);
};

AGServerSocket.prototype._nextCallId = function () {
  return this._cid++;
};

AGServerSocket.prototype.getState = function () {
  return this.state;
};

AGServerSocket.prototype.getBytesReceived = function () {
  return this.socket.bytesReceived;
};

AGServerSocket.prototype.emitError = function (error) {
  this.emit('error', {error});
};

AGServerSocket.prototype._onClose = function (code, reason) {
  clearInterval(this._pingIntervalTicker);
  clearTimeout(this._pingTimeoutTicker);

  if (this.state !== this.CLOSED) {
    let prevState = this.state;
    this.state = this.CLOSED;

    if (prevState === this.CONNECTING) {
      this.emit('connectAbort', {code, reason});
    } else {
      this.emit('disconnect', {code, reason});
    }
    this.emit('close', {code, reason});

    if (!AGServerSocket.ignoreStatuses[code]) {
      let closeMessage;
      if (reason) {
        let reasonString;
        if (typeof reason === 'object') {
          try {
            reasonString = JSON.stringify(reason);
          } catch (error) {
            reasonString = reason.toString();
          }
        } else {
          reasonString = reason;
        }
        closeMessage = `Socket connection closed with status code ${code} and reason: ${reasonString}`;
      } else {
        closeMessage = `Socket connection closed with status code ${code}`;
      }
      let err = new SocketProtocolError(AGServerSocket.errorStatuses[code] || closeMessage, code);
      this.emitError(err);
    }
  }
};

AGServerSocket.prototype.disconnect = function (code, data) {
  code = code || 1000;

  if (typeof code !== 'number') {
    let err = new InvalidArgumentsError('If specified, the code argument must be a number');
    this.emitError(err);
  }

  if (this.state !== this.CLOSED) {
    this._onClose(code, data);
    this.socket.close(code, data);
  }
};

AGServerSocket.prototype.terminate = function () {
  this.socket.terminate();
};

AGServerSocket.prototype.send = function (data, options) {
  this.socket.send(data, options, (err) => {
    if (err) {
      this._onClose(1006, err.toString());
    }
  });
};

AGServerSocket.prototype.decode = function (message) {
  return this.server.codec.decode(message);
};

AGServerSocket.prototype.encode = function (object) {
  return this.server.codec.encode(object);
};

AGServerSocket.prototype.sendObjectBatch = function (object) {
  this._batchSendList.push(object);
  if (this._batchTimeout) {
    return;
  }

  this._batchTimeout = setTimeout(() => {
    delete this._batchTimeout;
    if (this._batchSendList.length) {
      let str;
      try {
        str = this.encode(this._batchSendList);
      } catch (err) {
        this.emitError(err);
      }
      if (str != null) {
        this.send(str);
      }
      this._batchSendList = [];
    }
  }, this.server.options.pubSubBatchDuration || 0);
};

AGServerSocket.prototype.sendObjectSingle = function (object) {
  let str;
  try {
    str = this.encode(object);
  } catch (err) {
    this.emitError(err);
  }
  if (str != null) {
    this.send(str);
  }
};

AGServerSocket.prototype.sendObject = function (object, options) {
  if (options && options.batch) {
    this.sendObjectBatch(object);
  } else {
    this.sendObjectSingle(object);
  }
};

AGServerSocket.prototype.transmit = async function (event, data, options) {
  let newData;
  if (event === '#publish') {
    try {
      newData = await this._processOutboundPacket(event, data, options);
    } catch (error) {
      return;
    }
  } else {
    newData = data;
  }

  if (options && options.useCache && options.stringifiedData != null) {
    // Optimized
    this.send(options.stringifiedData);
  } else {
    let eventObject = {
      event
    };
    if (newData !== undefined) {
      eventObject.data = newData;
    }
    this.sendObject(eventObject);
  }
};

AGServerSocket.prototype.invoke = function (event, data, options) {
  return new Promise((resolve, reject) => {
    let eventObject = {
      event,
      cid: this._nextCallId()
    };
    if (newData !== undefined) {
      eventObject.data = newData;
    }

    let timeout = setTimeout(() => {
      let error = new TimeoutError(`Event response for "${event}" timed out`);
      delete this._callbackMap[eventObject.cid];
      reject(error);
    }, this.server.ackTimeout);

    this._callbackMap[eventObject.cid] = {
      callback: (err, result) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(result);
      },
      timeout
    };

    if (options && options.useCache && options.stringifiedData != null) {
      // Optimized
      this.send(options.stringifiedData);
    } else {
      this.sendObject(eventObject);
    }
  });
};

AGServerSocket.prototype.triggerAuthenticationEvents = function (oldAuthState) {
  if (oldAuthState !== this.AUTHENTICATED) {
    let stateChangeData = {
      oldAuthState,
      newAuthState: this.authState,
      authToken: this.authToken
    };
    this.emit('authStateChange', stateChangeData);
    this.server.emit('authenticationStateChange', {
      socket: this,
      ...stateChangeData
    });
  }
  this.emit('authenticate', {authToken: this.authToken});
  this.server.emit('authentication', {
    socket: this,
    authToken: this.authToken
  });
};

AGServerSocket.prototype.setAuthToken = async function (data, options) {
  let authToken = cloneDeep(data);
  let oldAuthState = this.authState;
  this.authState = this.AUTHENTICATED;

  if (options == null) {
    options = {};
  } else {
    options = cloneDeep(options);
    if (options.algorithm != null) {
      delete options.algorithm;
      let err = new InvalidArgumentsError(
        'Cannot change auth token algorithm at runtime - It must be specified as a config option on launch'
      );
      this.emitError(err);
    }
  }

  options.mutatePayload = true;
  let rejectOnFailedDelivery = options.rejectOnFailedDelivery;
  delete options.rejectOnFailedDelivery;
  let defaultSignatureOptions = this.server.defaultSignatureOptions;

  // We cannot have the exp claim on the token and the expiresIn option
  // set at the same time or else auth.signToken will throw an error.
  let expiresIn;
  if (options.expiresIn == null) {
    expiresIn = defaultSignatureOptions.expiresIn;
  } else {
    expiresIn = options.expiresIn;
  }
  if (authToken) {
    if (authToken.exp == null) {
      options.expiresIn = expiresIn;
    } else {
      delete options.expiresIn;
    }
  } else {
    options.expiresIn = expiresIn;
  }

  // Always use the default sync/async signing mode since it cannot be changed at runtime.
  if (defaultSignatureOptions.async != null) {
    options.async = defaultSignatureOptions.async;
  }
  // Always use the default algorithm since it cannot be changed at runtime.
  if (defaultSignatureOptions.algorithm != null) {
    options.algorithm = defaultSignatureOptions.algorithm;
  }

  this.authToken = authToken;

  let handleAuthTokenSignFail = (error) => {
    this.emitError(error);
    this._onClose(4002, error.toString());
    this.socket.close(4002);
    throw error;
  };

  let sendAuthTokenToClient = async (signedToken) => {
    let tokenData = {
      token: signedToken
    };
    try {
      return await this.invoke('#setAuthToken', tokenData);
    } catch (err) {
      throw new AuthError(`Failed to deliver auth token to client - ${err}`);
    }
  };

  let signTokenResult;

  try {
    signTokenResult = this.server.auth.signToken(authToken, this.server.signatureKey, options);
  } catch (err) {
    handleAuthTokenSignFail(err);
  }

  let signedAuthToken;
  if (signTokenResult instanceof Promise) {
    try {
      signedAuthToken = await signTokenResult;
    } catch (err) {
      handleAuthTokenSignFail(err);
    }
  } else {
    signedAuthToken = signTokenResult;
  }
  if (this.authToken === authToken) {
    this.signedAuthToken = signedAuthToken;
    this.emit('authTokenSigned', {signedAuthToken});
  }

  this.triggerAuthenticationEvents(oldAuthState);
  try {
    await sendAuthTokenToClient(signedAuthToken);
  } catch (err) {
    this.emitError(err);
    if (rejectOnFailedDelivery) {
      throw err;
    }
  }
};

AGServerSocket.prototype.getAuthToken = function () {
  return this.authToken;
};

AGServerSocket.prototype.deauthenticateSelf = function () {
  let oldAuthState = this.authState;
  let oldAuthToken = this.authToken;
  this.signedAuthToken = null;
  this.authToken = null;
  this.authState = this.UNAUTHENTICATED;
  if (oldAuthState !== this.UNAUTHENTICATED) {
    let stateChangeData = {
      oldAuthState,
      newAuthState: this.authState
    };
    this.emit('authStateChange', stateChangeData);
    this.server.emit('authenticationStateChange', {
      socket: this,
      ...stateChangeData
    });
  }
  this.emit('deauthenticate', {oldAuthToken});
  this.server.emit('deauthentication', {
    socket: this,
    oldAuthToken
  });
};

AGServerSocket.prototype.deauthenticate = function () {
  this.deauthenticateSelf();
  return this.invoke('#removeAuthToken');
};

AGServerSocket.prototype.kickOut = function (channel, message) {
  if (channel == null) {
    Object.keys(this.channelSubscriptions).forEach((channelName) => {
      delete this.channelSubscriptions[channelName];
      this.channelSubscriptionsCount--;
      this.transmit('#kickOut', {message: message, channel: channelName});
    });
  } else {
    delete this.channelSubscriptions[channel];
    this.channelSubscriptionsCount--;
    this.transmit('#kickOut', {message: message, channel: channel});
  }
  return this.server.brokerEngine.unsubscribeSocket(this, channel);
};

AGServerSocket.prototype.subscriptions = function () {
  return Object.keys(this.channelSubscriptions);
};

AGServerSocket.prototype.isSubscribed = function (channel) {
  return !!this.channelSubscriptions[channel];
};

AGServerSocket.prototype._processPublishAction = function (options, request, callback) {
  let callbackInvoked = false;

  if (this.server.allowClientPublish) {
    let eventData = options.data || {};
    request.channel = eventData.channel;
    request.data = eventData.data;
    request.block = (error) => {

    };

    this._middlewareInboundStream.write(request);
    // TODO 2
    // async.applyEachSeries(this._middleware[this.MIDDLEWARE_PUBLISH_IN], request,
    //   (err) => {
    //     if (callbackInvoked) {
    //       this.emitError(
    //         new InvalidActionError(
    //           `Callback for ${this.MIDDLEWARE_PUBLISH_IN} middleware was already invoked`
    //         )
    //       );
    //     } else {
    //       callbackInvoked = true;
    //       if (request.data !== undefined) {
    //         eventData.data = request.data;
    //       }
    //       if (err) {
    //         if (err === true || err.silent) {
    //           err = new SilentMiddlewareBlockedError(
    //             `Action was silently blocked by ${this.MIDDLEWARE_PUBLISH_IN} middleware`,
    //             this.MIDDLEWARE_PUBLISH_IN
    //           );
    //         } else if (this.server.middlewareEmitWarnings) {
    //           this.emitError(err);
    //         }
    //         callback(err, eventData, request.ackData);
    //       } else {
    //         if (typeof request.channel !== 'string') {
    //           err = new BrokerError(
    //             `Socket ${request.socket.id} tried to publish to an invalid ${request.channel} channel`
    //           );
    //           this.emitError(err);
    //           callback(err, eventData, request.ackData);
    //           return;
    //         }
    //         (async () => {
    //           let error;
    //           try {
    //             await this.server.exchange.publish(request.channel, request.data);
    //           } catch (err) {
    //             error = err;
    //             this.emitError(error);
    //           }
    //           callback(error, eventData, request.ackData);
    //         })();
    //       }
    //     }
    //   }
    // );
  } else {
    let noPublishError = new InvalidActionError('Client publish feature is disabled');
    this.emitError(noPublishError);
    callback(noPublishError);
  }
};

AGServerSocket.prototype._processSubscribeAction = function (options, request, callback) {
  let callbackInvoked = false;

  let eventData = options.data || {};
  request.channel = eventData.channel;
  request.waitForAuth = eventData.waitForAuth;
  request.data = eventData.data;

  if (request.waitForAuth && request.authTokenExpiredError) {
    // If the channel has the waitForAuth flag set, then we will handle the expiry quietly
    // and we won't pass this request through the subscribe middleware.
    callback(request.authTokenExpiredError, eventData);
  } else {
    async.applyEachSeries(this._middleware[this.MIDDLEWARE_SUBSCRIBE], request,
      (err) => {
        if (callbackInvoked) {
          this.emitError(
            new InvalidActionError(
              `Callback for ${this.MIDDLEWARE_SUBSCRIBE} middleware was already invoked`
            )
          );
        } else {
          callbackInvoked = true;
          if (err) {
            if (err === true || err.silent) {
              err = new SilentMiddlewareBlockedError(
                `Action was silently blocked by ${this.MIDDLEWARE_SUBSCRIBE} middleware`,
                this.MIDDLEWARE_SUBSCRIBE
              );
            } else if (this.server.middlewareEmitWarnings) {
              this.emitError(err);
            }
          }
          if (request.data !== undefined) {
            eventData.data = request.data;
          }
          callback(err, eventData);
        }
      }
    );
  }
};

AGServerSocket.prototype._processTransmitAction = function (options, request, callback) {
  let callbackInvoked = false;

  request.event = options.event;
  request.data = options.data;

  async.applyEachSeries(this._middleware[this.MIDDLEWARE_TRANSMIT], request,
    (err) => {
      if (callbackInvoked) {
        this.emitError(
          new InvalidActionError(
            `Callback for ${this.MIDDLEWARE_TRANSMIT} middleware was already invoked`
          )
        );
      } else {
        callbackInvoked = true;
        if (err) {
          if (err === true || err.silent) {
            err = new SilentMiddlewareBlockedError(
              `Action was silently blocked by ${this.MIDDLEWARE_TRANSMIT} middleware`,
              this.MIDDLEWARE_TRANSMIT
            );
          } else if (this.server.middlewareEmitWarnings) {
            this.emitError(err);
          }
        }
        callback(err, request.data);
      }
    }
  );
};

AGServerSocket.prototype._processInvokeAction = function (options, request, callback) {
  let callbackInvoked = false;

  request.event = options.event;
  request.data = options.data;

  // TODO 2: Remove async module and use for-await-of loop instead
  async.applyEachSeries(this._middleware[this.MIDDLEWARE_INVOKE], request,
    (err) => {
      if (callbackInvoked) {
        this.emitError(
          new InvalidActionError(
            `Callback for ${this.MIDDLEWARE_INVOKE} middleware was already invoked`
          )
        );
      } else {
        callbackInvoked = true;
        if (err) {
          if (err === true || err.silent) {
            err = new SilentMiddlewareBlockedError(
              `Action was silently blocked by ${this.MIDDLEWARE_INVOKE} middleware`,
              this.MIDDLEWARE_INVOKE
            );
          } else if (this.server.middlewareEmitWarnings) {
            this.emitError(err);
          }
        }
        callback(err, request.data);
      }
    }
  );
};

AGServerSocket.prototype._isReservedRemoteEvent = function (event) {
  return typeof event === 'string' && event.charAt(0) === '#';
};

AGServerSocket.prototype._processAuthTokenExpiry = function () {
  let token = this.getAuthToken();
  if (this.isAuthTokenExpired(token)) {
    this.deauthenticate();

    return new AuthTokenExpiredError(
      'The socket auth token has expired',
      token.exp
    );
  }
  return null;
};

// TODO 2: DELETE
AGServerSocket.prototype._verifyInboundAction = function (action, callback) {
  let token = this.getAuthToken();
  if (this.isAuthTokenExpired(token)) {
    requestOptions.authTokenExpiredError = new AuthTokenExpiredError(
      'The socket auth token has expired',
      token.exp
    );

    this.deauthenticate();
  }

  this._passThroughMiddleware(requestOptions, callback);
};

AGServerSocket.prototype.isAuthTokenExpired = function (token) {
  if (token && token.exp != null) {
    let currentTime = Date.now();
    let expiryMilliseconds = token.exp * 1000;
    return currentTime > expiryMilliseconds;
  }
  return false;
};

// TODO 2 DELETE
AGServerSocket.prototype._passThroughMiddleware = function (options, callback) {
  let request = {};

  if (options.authTokenExpiredError != null) {
    request.authTokenExpiredError = options.authTokenExpiredError;
  }

  let event = options.event;

  if (options.cid == null) {
    // If transmit.
    if (this._isReservedRemoteEvent(event)) {
      if (event === '#publish') {
        this._processPublishAction(options, request, callback);
      } else if (event === '#removeAuthToken') {
        callback(null, options.data);
      } else {
        let error = new InvalidActionError(`The reserved transmitted event ${event} is not supported`);
        callback(error);
      }
    } else {
      this._processTransmitAction(options, request, callback);
    }
  } else {
    // If invoke/RPC.
    if (this._isReservedRemoteEvent(event)) {
      if (event === '#subscribe') {
        this._processSubscribeAction(options, request, callback);
      } else if (event === '#publish') {
        this._processPublishAction(options, request, callback);
      } else if (
        event === '#handshake' ||
        event === '#authenticate' ||
        event === '#unsubscribe'
      ) {
        callback(null, options.data);
      } else {
        let error = new InvalidActionError(`The reserved invoked event ${event} is not supported`);
        callback(error);
      }
    } else {
      this._processInvokeAction(options, request, callback);
    }
  }
};

AGServerSocket.prototype._processTokenError = function (err) {
  let authError = null;
  let isBadToken = true;

  if (err) {
    if (err.name === 'TokenExpiredError') {
      authError = new AuthTokenExpiredError(err.message, err.expiredAt);
    } else if (err.name === 'JsonWebTokenError') {
      authError = new AuthTokenInvalidError(err.message);
    } else if (err.name === 'NotBeforeError') {
      authError = new AuthTokenNotBeforeError(err.message, err.date);
      // In this case, the token is good; it's just not active yet.
      isBadToken = false;
    } else {
      authError = new AuthTokenError(err.message);
    }
  }

  return {
    authError,
    isBadToken
  };
};

AGServerSocket.prototype._emitBadAuthTokenError = function (error, signedAuthToken) {
  this.emit('badAuthToken', {
    authError: error,
    signedAuthToken
  });
  this.server.emit('badSocketAuthToken', {
    socket: this,
    authError: error,
    signedAuthToken
  });
};

AGServerSocket.prototype._passThroughAuthenticateMiddleware = function (options, callback) {
  let callbackInvoked = false;

  let request = {
    authToken: options.authToken
  };

  async.applyEachSeries(this._middleware[this.MIDDLEWARE_AUTHENTICATE], request,
    (err, results) => {
      if (callbackInvoked) {
        this.emitError(
          new InvalidActionError(
            `Callback for ${this.MIDDLEWARE_AUTHENTICATE} middleware was already invoked`
          )
        );
      } else {
        callbackInvoked = true;
        let isBadToken = false;
        if (results.length) {
          isBadToken = results[results.length - 1] || false;
        }
        if (err) {
          if (err === true || err.silent) {
            err = new SilentMiddlewareBlockedError(
              `Action was silently blocked by ${this.MIDDLEWARE_AUTHENTICATE} middleware`,
              this.MIDDLEWARE_AUTHENTICATE
            );
          } else if (this.server.middlewareEmitWarnings) { // TODO 2: Rename middlewareEmitWarnings to middlewareEmitFailures
            this.emitError(err);
          }
        }
        callback(err, isBadToken);
      }
    }
  );
};

AGServerSocket.prototype.processAuthToken = async function (signedAuthToken) {
  let verificationOptions = Object.assign({}, this.server.defaultVerificationOptions);

  let handleVerifyTokenResult = async (result) => {
    let err = result.error;
    let token = result.token;

    let oldAuthState = this.authState;
    if (token) {
      this.signedAuthToken = signedAuthToken;
      this.authToken = token;
      this.authState = this.AUTHENTICATED;
    } else {
      this.signedAuthToken = null;
      this.authToken = null;
      this.authState = this.UNAUTHENTICATED;
    }

    // If the socket is authenticated, pass it through the MIDDLEWARE_AUTHENTICATE middleware.
    // If the token is bad, we will tell the client to remove it.
    // If there is an error but the token is good, then we will send back a 'quiet' error instead
    // (as part of the status object only).
    if (this.authToken) {
      let action = new Action(this.MIDDLEWARE_AUTHENTICATE, {
        signedAuthToken: this.signedAuthToken,
        authToken: this.authToken
      });

      try {
        await this._processMiddlewareAction(this._middlewareInboundStream, action);
      } catch (error) {

      }

      // TODO 6

      this._passThroughAuthenticateMiddleware({
        signedAuthToken: this.signedAuthToken,
        authToken: this.authToken
      }, (middlewareError, isBadToken) => {
        if (middlewareError) {
          this.authToken = null;
          this.authState = this.UNAUTHENTICATED;
          if (isBadToken) { // TODO 2: Check if isBadToken functionality is correct <-----------------
            this._emitBadAuthTokenError(middlewareError, signedAuthToken);
          }
        }
        // If an error is passed back from the authenticate middleware, it will be treated as a
        // server warning and not a socket error.
        callback(middlewareError, isBadToken || false, oldAuthState);
      });
    } else {
      let errorData = this._processTokenError(err);

      // If the error is related to the JWT being badly formatted, then we will
      // treat the error as a socket error.
      if (err && signedAuthToken != null) {
        this.emitError(errorData.authError);
        if (errorData.isBadToken) {
          this._emitBadAuthTokenError(errorData.authError, signedAuthToken);
        }
      }
      callback(errorData.authError, errorData.isBadToken, oldAuthState);
    }
  };

  let verifyTokenResult;
  let verifyTokenError;

  try {
    verifyTokenResult = this.server.auth.verifyToken(signedAuthToken, this.verificationKey, verificationOptions);
  } catch (err) {
    verifyTokenError = err;
  }

  if (verifyTokenResult instanceof Promise) {
    (async () => {
      let result = {};
      try {
        result.token = await verifyTokenResult;
      } catch (err) {
        result.error = err;
      }
      handleVerifyTokenResult(result);
    })();
  } else {
    let result = {
      token: verifyTokenResult,
      error: verifyTokenError
    };
    handleVerifyTokenResult(result);
  }
};

AGServerSocket.prototype._verifyOutboundAction = function (eventName, eventData, options, callback) { // TODO 4
  let callbackInvoked = false;

  if (eventName === '#publish') {
    let request = {
      channel: eventData.channel,
      data: eventData.data
    };
    async.applyEachSeries(this._middleware[this.MIDDLEWARE_PUBLISH_OUT], request,
      (err) => {
        if (callbackInvoked) {
          this.emitError(
            new InvalidActionError(
              `Callback for ${this.MIDDLEWARE_PUBLISH_OUT} middleware was already invoked`
            )
          );
        } else {
          callbackInvoked = true;
          if (request.data !== undefined) {
            eventData.data = request.data;
          }
          if (err) {
            if (err === true || err.silent) {
              err = new SilentMiddlewareBlockedError(
                `Action was silently blocked by ${this.MIDDLEWARE_PUBLISH_OUT} middleware`,
                this.MIDDLEWARE_PUBLISH_OUT
              );
            } else if (this.server.middlewareEmitWarnings) {
              this.emitError(err);
            }
            callback(err, eventData);
          } else {
            if (options && request.useCache) {
              options.useCache = true;
            }
            callback(null, eventData);
          }
        }
      }
    );
  } else {
    callback(null, eventData);
  }
};

module.exports = AGServerSocket;
