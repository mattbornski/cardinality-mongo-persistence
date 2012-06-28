var mongodb = require('mongodb');

var determineCollection = function (settings, callback) {
  if (settings['collection'] !== undefined) {
    // Is this a collection object?  Or a collection name?
    if (settings['collection'].toString() === settings['collection']) {
      // If it is a collection name, then it must be accompanied by a database of some kind.
      // TODO
      callback(new Error('No Mongo collection object provided, got name instead'));
      return;
    } else if (settings['collection'] instanceof mongodb.Collection) {
      callback(null, settings['collection']);
      return;
    } else {
      callback(new Error('Unable to determine Mongo collection from ' + settings['collection']));
      return;
    }
  } else {
    callback(new Error('No Mongo collection provided'));
  }
};

var determineDocument = function (settings, collection, callback) {
  if (settings['_id'] !== undefined) {
    callback(null, settings['_id']);
  } else if (settings['query'] !== undefined) {
    collection.findAndModify(settings['query'], {'upsert': true, 'new': true}, function (error, doc) {
      if (error) {
        callback(error);
        return;
      }
      if (doc) {

      } else {
        return;
      }
    });
  } else {
    callback(new Error('Must pass _id or query to determine document'));
  }
};

var flush = function (state, callback) {
  state['collection'].findOne({'_id': state['_id']}, function (error, doc) {
    if (error) {
      callback(error);
      return;
    }
    var update = {'$pullAll': {}};
    for (var i in (doc || {})['M']) {
      if (doc['M'][i].length > 1) {
        // Remove all elements from this set which are not the maximum element.
        update['$pullAll']['M.' + i] = doc['M'][i].sort(function(a, b) { return a - b}).slice(0, -1);
      }
      state['doc']['M'][i] = [doc['M'][i][doc['M'][i].length - 1]];
    }
    if (Object.keys(update['$pullAll']).length > 0) {
      state['collection'].update({'_id': update['_id']}, update);
    }
    callback(null);
  });
};

module.exports = {'persistence': {
  'name': 'mongo',

  // Settings must include enough information to connect to a Mongo instance.
  // If an _id field is specified, we'll create that document; otherwise,
  // we'll create a new one with an auto-assigned _id.
  'create': function (numBuckets, settings, callback) {
    determineCollection(settings, function (error, collection) {
      if (error) {
        callback(error);
        return;
      }
      determineDocument(collection, settings, function (error, document) {
        if (error) {
          callback(error);
          return;
        }
        var state = {
          'collection': collection,
          '_id': document['_id'],
        };
        var update = {
          '$addToSet': {},
        };
        for (var i = 0; i < numBuckets; i++) {
          update['$addToSet']['M.' + i.toString()] = [0];
        }
        state['collection'].insert({}, function (error, doc) {
          state['_id'] = doc['_id'];
        });
        callback(null, state);
      });
    });
  },
  // Restore an old table's state, which we are certain is from the same
  // type of persistence.
  'restore': function (state, settings, callback) {
    callback(null, {
      'M': state['M'],
      'count': state['count'],
    });
  },
  // Insert another key into the persistence mechanism.
  // The hash algorithm has already been applied.
  // This method is intended to be fire-and-forget; no callback given.
  'insert': function (state, bucketIndex, bucketValue, callback) {
    var update = {'$addToSet': {}, '$inc': {'count': 1}};
    update['$addToSet']['M.' + bucketIndex.toString()] = bucketValue;
    state['collection'].update({'_id': state['_id']}, update, {'upsert': true});

    // Return nothing
    callback(null, null);
  },
  // Finalize your data storage if necessary, then call callback.
  // Retrieve our buckets from our state, then pass them to the callback.
  'represent': function (state, callback) {
    flush(state, function (error) {
      if (error) {
        callback(error);
      } else {
        state['collection'].findOne({'_id': state['_id']}, function (error, doc) {
          if (error) {
            callback(error);
          } else {
            // Refresh in-memory state from the one we just retrieved.


            callback(null, {
              'M': state['M'],
              'count': state['count'],
            });
          }
        });
      }
    })
  },
}};

(function () {
  require('cardinality').register(module.exports['persistence']);
})();