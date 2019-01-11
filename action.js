const scErrors = require('sc-errors');
const InvalidActionError = scErrors.InvalidActionError;

function Action(type, data) {
  this.type = type;
  this.data = data;
  this.outcome = null;
  this.promise = new Promise((resolve, reject) => {
    this._resolve = resolve;
    this._reject = reject;
  });
}

Action.prototype.allow = function (data) {
  if (this.outcome) {
    throw new InvalidActionError(`Action ${this.id} has already been ${this.outcome}; cannot allow`);
  }
  this.outcome = 'allowed';
  this._resolve(data);
};

Action.prototype.block = function (error) {
  if (this.outcome) {
    throw new InvalidActionError(`Action ${this.id} has already been ${this.outcome}; cannot block`);
  }
  this.outcome = 'blocked';
  this._reject(error);
};

module.exports = Action;
