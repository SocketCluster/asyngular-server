const scErrors = require('sc-errors');
const InvalidActionError = scErrors.InvalidActionError;

function Action(type, data) {
  this.type = type;
  this.data = data;
  this.processed = false;
  this.promise = new Promise((resolve, reject) => {
    this._resolve = resolve;
    this._reject = reject;
  });
}

Action.prototype.allow = function (data) {
  if (this.processed) {
    throw new InvalidActionError(`Action ${this.id} has already been processed; cannot allow`);
  }
  this.processed = true;
  this._resolve(data);
};

Action.prototype.block = function (error) {
  if (this.processed) {
    throw new InvalidActionError(`Action ${this.id} has already been processed; cannot block`);
  }
  this.processed = true;
  this._reject(error);
};

module.exports = Action;
