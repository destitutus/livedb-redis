'use strict';

var RedisDb = module.exports = function(redis) {
    if (!(this instanceof RedisDb)) return new RedisDb();
    if (redis) {
        this.redis = redis;
    } else {
        var redisLib = require('redis');
        this.redis = redisLib.createClient();
    }
};

RedisDb.prototype.close = function() {
    if (this.redis) {
        this.redis.quit();
    }
};

function snapshotName(cName, docName) {
    return ['sjset', cName, docName].join('-');
}

function opsName(cName, docName, version) {
    return ['sjsetops', cName, docName, version].join('-');
}

function opsVersionName(cName, docName) {
    return ['sjsetopsversion', cName, docName].join('-');
}

RedisDb.prototype.writeSnapshot = function(cName, docName, snapshot, callback) {
    this.redis.set(snapshotName(cName, docName), JSON.stringify(snapshot), callback);
};

RedisDb.prototype.getSnapshot = function(cName, docName, callback) {
    this.redis.get(snapshotName(cName, docName), function (err, data) {
        callback && callback(err, JSON.parse(data));
    });
};

RedisDb.prototype.writeOp = function(cName, docName, opData, callback) {
    var self = this;
    var name = opsName(cName, docName, opData.v);
    this.redis.set(name, JSON.stringify(opData), function (err) {
        callback && callback(err);
        if (!err) {
            self.redis.set(opsVersionName(cName, docName), opData.v);
        }
    });
};


RedisDb.prototype.getVersion = function(cName, docName, callback) {
    this.redis.get(opsVersionName(cName, docName), function (err, data) {
        if (err || !data) {
            callback && callback(err, 0);
        } else {
            callback && callback(err, data);
        }
    });
};

RedisDb.prototype.getOps = function(cName, docName, start, end, callback) {
    var self = this;
    function processResult(err, data) {
        if (err || !data) {
            return callback && callback(err, data);
        }
        var ret = [];
        for (var pos in data) {
            var item = data[pos];
            if (item) {
                ret.push(JSON.parse(item));
            }
        }
        return callback && callback(err, ret);
    }
    function _getOps(iStart, iEnd) {
        if ((iStart !== 0 && !iStart) || (iEnd !== 0 && !iEnd)) {
            return processResult(undefined, []);
        }
        if (iEnd < iStart) {
            var tmp = iStart;
            iStart = iEnd;
            iEnd = tmp;
        }
        for (var i = iStart; i < iEnd + 1; i++) {
            keys.push(opsName(cName, docName, i));
        }
        if (keys.length === 0) {
            processResult(undefined, []);
        } else {
            self.redis.mget(keys.join(' '), processResult);
        }
    }
    var keys = [];
    if (end === null) {
        this.getVersion(cName, docName, function (err, version) {
            _getOps(start, version);
        });
    } else {
        _getOps(start, end);
    }
};

RedisDb.prototype.queryNeedsPollMode = function(index, query) {
    return false;
};

RedisDb.prototype.query = function(liveDb, index, query, options, callback) {
    callback('Not implemented');
};

RedisDb.prototype.queryDoc = function(liveDb, index, cName, docName, query, callback) {
    callback('Not implemented');
};

