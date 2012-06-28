
/* Mocha test
   to use:
     npm install mocha
     mocha <filename>
   or
     npm test
*/

var assert = require('assert');
var cardinality = require('cardinality');
require('../lib/index');
var mongodb = require('mongodb');

var makeData = function (size) {
  var result = [];

  var count = size;
  while (count > 0) {
    var word = '';
    for (var j = 0; j < 24; j++) {
      word += '0123456789abcdef'[Math.floor(Math.random() * 16)];
    }

    for (var i = 0; i < Math.random() * 2; i++) {
      result.push(word);
      count--;
    }
  }
  return result;
};

describe('Mongo-backed cardinality set', function () {
  it('should yield same estimate as straight cardinality set', function (done) {
    var db = new mongodb.Db('test', new mongodb.Server('127.0.0.1', 27017));
    db.open(function (error, db) {
      if (error) {
        return done(error);
      }
      db.collection('cardinalityTest_' + Date.now(), function (error, coll) {
        if (error) {
          return done(error);
        }
        var size = 100000;
        var data = makeData(size);
        console.log('for set of size ~' + size + '...');
        var mongoStart = Date.now();
        cardinality.set(data, {'persistence': 'mongo', 'collection': coll}).size(function (error, mongoCount) {
          var mongoEnd = Date.now();
          if (error) {
            return done(error);
          }
          console.log('  Mongo persistence estimated ' + mongoCount + ' items in ' + mongoEnd - mongoStart + ' ms');
          var straight = (new cardinality.set(data)).size(function (error, straightCount) {
            console.log('  Naive counting netted ' + results[index]['counts'][0] + ' items in ' + results[index]['times'][0] + ' ms');
            var deviation = (Math.abs(results[index]['counts'][0] - results[index]['counts'][1]) / results[index]['counts'][0]);
            console.log(deviation);
            // The allowable deviation for this test is pretty high:
            assert(deviation <= 0.25);
            return done();
          });
        });
      });
    });
  });
});

/*describe('Hyper Log Log algorithm', function () {
  it('with BSON object id strings', function (done) {
    var sizes = [
      100000,
    ];
    var results = harness.compare(harness.lightlyOverlappingObjectIds, harness.naiveCardinality, cardinality.set, sizes);
    for (var index in sizes) {
      console.log('for set of size ~' + sizes[index] + '...');
      console.log('  HyperLogLog estimated ' + results[index]['counts'][1] + ' items in ' + results[index]['times'][1] + ' ms');
      console.log('  Naive counting netted ' + results[index]['counts'][0] + ' items in ' + results[index]['times'][0] + ' ms');
      var deviation = (Math.abs(results[index]['counts'][0] - results[index]['counts'][1]) / results[index]['counts'][0]);
      console.log(deviation);
      // The allowable deviation for this test is pretty high:
      assert(deviation <= 0.25);
    }
    return done();
  });
});*/