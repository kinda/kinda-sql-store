'use strict';

var _ = require('lodash');
var util = require('kinda-util').create();
var AbstractStore = require('kinda-abstract-store');

var DEFAULT_LIMIT = 50000;

var SQLStore = AbstractStore.extend('SQLStore', function() {
  this.get = function *(key, options) {
    key = this.normalizeKey(key);
    if (!options) options = {};
    if (!options.hasOwnProperty('errorIfMissing'))
      options.errorIfMissing = true;
    yield this.initializeDatabase();
    var sql = 'SELECT `value` FROM `pairs` WHERE `key`=?';
    var res = yield this.connection.query(sql, [this.encodeKey(key)]);
    if (!res.length) {
      if (options.errorIfMissing) throw new Error('item not found');
      return;
    }
    return this.decodeValue(res[0].value);
  };

  this.put = function *(key, value, options) {
    key = this.normalizeKey(key);
    if (!options) options = {};
    if (!options.hasOwnProperty('createIfMissing'))
      options.createIfMissing = true;
    yield this.initializeDatabase();
    var encodedKey = this.encodeKey(key);
    var encodedValue = this.encodeValue(value);
    var sql;
    if (options.errorIfExists) {
      sql = 'INSERT INTO `pairs` (`key`, `value`) VALUES(?,?)';
      yield this.connection.query(sql, [encodedKey, encodedValue]);
    } else if (options.createIfMissing) {
      sql = 'REPLACE INTO `pairs` (`key`, `value`) VALUES(?,?)';
      yield this.connection.query(sql, [encodedKey, encodedValue]);
    } else {
      sql = 'UPDATE `pairs` SET `value`=? WHERE `key`=?';
      var res = yield this.connection.query(sql, [encodedValue, encodedKey]);
      if (!res.affectedRows) throw new Error('item not found');
    }
  };

  this.del = function *(key, options) {
    key = this.normalizeKey(key);
    if (!options) options = {};
    if (!options.hasOwnProperty('errorIfMissing'))
      options.errorIfMissing = true;
    yield this.initializeDatabase();
    var sql = 'DELETE FROM `pairs` WHERE `key`=?';
    var res = yield this.connection.query(sql, [this.encodeKey(key)]);
    if (!res.affectedRows && options.errorIfMissing)
      throw new Error('item not found');
  };

  this.getMany = function *(keys, options) {
    if (!_.isArray(keys))
      throw new Error('invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    if (!options) options = {};
    if (!options.hasOwnProperty('errorIfMissing'))
      options.errorIfMissing = true;
    if (!options.hasOwnProperty('returnValues'))
      options.returnValues = true;

    yield this.initializeDatabase();

    var results;
    var resultsMap = {};

    var encodedKeys = keys.map(this.encodeKey, this);
    while (encodedKeys.length) {
      var someKeys = encodedKeys.splice(0, 500); // take 500 keys
      if (someKeys.length) {
        var placeholders = '';
        for (var i = 0; i < someKeys.length; i++) {
          if (i > 0) placeholders += ',';
          placeholders += '?';
        }
        var what = options.returnValues ? '*' : '`key`';
        var where = '`key` IN (' + placeholders + ')';
        var sql = 'SELECT ' + what + ' FROM `pairs` WHERE ' + where;
        results = yield this.connection.query(sql, someKeys);
        results.forEach(function(item) {
          var key = this.decodeKey(item.key);
          var res = { key: key };
          if (options.returnValues)
           res.value = this.decodeValue(item.value);
          resultsMap[key.toString()] = res;
        }, this);
      }
    }

    results = [];
    keys.forEach(function(key) {
      var res = resultsMap[key.toString()];
      if (res) results.push(res);
    });

    if (results.length !== keys.length && options.errorIfMissing)
      throw new Error('some items not found');

    return results;
  };

  this.putMany = function *(items, options) {
    // TODO
  };

  this.delMany = function *(key, options) {
    // TODO
  };

  // options: prefix, start, startAfter, end, endBefore,
  //   reverse, limit, returnValues
  this.getRange = function *(options) {
    if (!options) options = {};
    options = this.normalizeKeySelectors(options);
    var limit;
    if (options.hasOwnProperty('limit'))
      limit = Number(options.limit);
    else
      limit = DEFAULT_LIMIT;
    if (!options.hasOwnProperty('returnValues'))
      options.returnValues = true;
    yield this.initializeDatabase();
    var what = options.returnValues ? '*' : '`key`';
    var sql = 'SELECT ' + what + ' FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    sql += ' ORDER BY `key`' + (options.reverse ? ' DESC' : '');
    sql += ' LIMIT ' + limit;
    var res = yield this.connection.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    var items = res.map(function(item) {
      var res = { key: this.decodeKey(item.key) };
      if (options.returnValues)
       res.value = this.decodeValue(item.value);
      return res;
    }, this);
    return items;
  };

  // options: prefix, start, startAfter, end, endBefore
  this.getCount = function *(options) {
    if (!options) options = {};
    options = this.normalizeKeySelectors(options);
    yield this.initializeDatabase();
    var sql = 'SELECT COUNT(*) FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    var res = yield this.connection.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    if (res.length !== 1) throw new Error('invalid result');
    if (!res[0].hasOwnProperty('COUNT(*)')) throw new Error('invalid result');
    return res[0]['COUNT(*)'];
  };

  this.delRange = function *(options) {
    if (!options) options = {};
    options = this.normalizeKeySelectors(options);
    yield this.initializeDatabase();
    var sql = 'DELETE FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    var res = yield this.connection.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    return res.affectedRows;
  };
});

module.exports = SQLStore;
