'use strict';

let _ = require('lodash');
let AbstractStore = require('kinda-abstract-store');

const DEFAULT_LIMIT = 50000;

let SQLStore = AbstractStore.extend('SQLStore', function() {
  this.get = function *(key, options = {}) {
    key = this.normalizeKey(key);
    _.defaults(options, { errorIfMissing: true });
    yield this.initializeDatabase();
    let sql = 'SELECT `value` FROM `pairs` WHERE `key`=?';
    let res = yield this.connection.query(sql, [this.encodeKey(key)]);
    if (!res.length) {
      if (options.errorIfMissing) throw new Error('item not found');
      return undefined;
    }
    return this.decodeValue(res[0].value);
  };

  this.put = function *(key, value, options = {}) {
    key = this.normalizeKey(key);
    _.defaults(options, { errorIfMissing: true });
    yield this.initializeDatabase();
    let encodedKey = this.encodeKey(key);
    let encodedValue = this.encodeValue(value);
    let sql;
    if (options.errorIfExists) {
      sql = 'INSERT INTO `pairs` (`key`, `value`) VALUES(?,?)';
      yield this.connection.query(sql, [encodedKey, encodedValue]);
    } else if (options.createIfMissing) {
      sql = 'REPLACE INTO `pairs` (`key`, `value`) VALUES(?,?)';
      yield this.connection.query(sql, [encodedKey, encodedValue]);
    } else {
      sql = 'UPDATE `pairs` SET `value`=? WHERE `key`=?';
      let res = yield this.connection.query(sql, [encodedValue, encodedKey]);
      if (!res.affectedRows) throw new Error('item not found');
    }
  };

  this.del = function *(key, options = {}) {
    key = this.normalizeKey(key);
    _.defaults(options, { errorIfMissing: true });
    yield this.initializeDatabase();
    let sql = 'DELETE FROM `pairs` WHERE `key`=?';
    let res = yield this.connection.query(sql, [this.encodeKey(key)]);
    if (!res.affectedRows && options.errorIfMissing) {
      throw new Error('item not found (key=\'' + JSON.stringify(key) + '\')');
    }
    return !!res.affectedRows;
  };

  this.getMany = function *(keys, options = {}) {
    if (!_.isArray(keys)) throw new Error('invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    _.defaults(options, { errorIfMissing: true, returnValues: true });

    yield this.initializeDatabase();

    let results;
    let resultsMap = {};

    let encodedKeys = keys.map(this.encodeKey, this);
    while (encodedKeys.length) {
      let someKeys = encodedKeys.splice(0, 500); // take 500 keys
      if (someKeys.length) {
        let placeholders = '';
        for (let i = 0; i < someKeys.length; i++) {
          if (i > 0) placeholders += ',';
          placeholders += '?';
        }
        let what = options.returnValues ? '*' : '`key`';
        let where = '`key` IN (' + placeholders + ')';
        let sql = 'SELECT ' + what + ' FROM `pairs` WHERE ' + where;
        results = yield this.connection.query(sql, someKeys);
        results.forEach(function(item) { // eslint-disable-line no-loop-func
          let key = this.decodeKey(item.key);
          let res = { key };
          if (options.returnValues) res.value = this.decodeValue(item.value);
          resultsMap[key.toString()] = res;
        }, this);
      }
    }

    results = [];
    keys.forEach(function(key) {
      let res = resultsMap[key.toString()];
      if (res) results.push(res);
    });

    if (results.length !== keys.length && options.errorIfMissing) {
      throw new Error('some items not found');
    }

    return results;
  };

  this.putMany = function *(items, options = {}) { // eslint-disable-line
    // TODO
  };

  this.delMany = function *(key, options = {}) { // eslint-disable-line
    // TODO
  };

  // options: prefix, start, startAfter, end, endBefore,
  //   reverse, limit, returnValues
  this.getRange = function *(options = {}) {
    options = this.normalizeKeySelectors(options);
    _.defaults(options, { limit: DEFAULT_LIMIT, returnValues: true });
    yield this.initializeDatabase();
    let what = options.returnValues ? '*' : '`key`';
    let sql = 'SELECT ' + what + ' FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    sql += ' ORDER BY `key`' + (options.reverse ? ' DESC' : '');
    sql += ' LIMIT ' + options.limit;
    let items = yield this.connection.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    items = items.map(function(item) {
      let res = { key: this.decodeKey(item.key) };
      if (options.returnValues) res.value = this.decodeValue(item.value);
      return res;
    }, this);
    return items;
  };

  // options: prefix, start, startAfter, end, endBefore
  this.getCount = function *(options = {}) {
    options = this.normalizeKeySelectors(options);
    yield this.initializeDatabase();
    let sql = 'SELECT COUNT(*) FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    let res = yield this.connection.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    if (res.length !== 1) throw new Error('invalid result');
    if (!res[0].hasOwnProperty('COUNT(*)')) throw new Error('invalid result');
    return res[0]['COUNT(*)'];
  };

  this.delRange = function *(options = {}) {
    options = this.normalizeKeySelectors(options);
    yield this.initializeDatabase();
    let sql = 'DELETE FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    let res = yield this.connection.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    return res.affectedRows;
  };
});

module.exports = SQLStore;
