const scErrors = require('sc-errors');
const InvalidActionError = scErrors.InvalidActionError;

function Request(socket, id, data) {
  this.socket = socket;
  this.id = id;
  this.data = data;
  this.sent = false;
}

Request.prototype._respond = function (responseData, options) {
  if (this.sent) {
    throw new InvalidActionError(`Response to request ${this.id} has already been sent`);
  }
  this.sent = true;
  this.socket.sendObject(responseData, options);
};

Request.prototype.end = function (data, options) {
  let responseData = {
    rid: this.id
  };
  if (data !== undefined) {
    responseData.data = data;
  }
  this._respond(responseData, options);
};

Request.prototype.error = function (error, data, options) {
  let err = scErrors.dehydrateError(error);

  let responseData = {
    rid: this.id,
    error: err
  };
  if (data !== undefined) {
    responseData.data = data;
  }

  this._respond(responseData, options);
};

module.exports = Request;
