/***
*  Title:         A directory service for context-related information
*  Function:      Directory service
*  Author:        Sebastian Wilkes
*  Email:         sebastianwilkes@gmail.com
*  Last edited:   03.07.2018
***/

const express         = require('express');
const bodyParser      = require('body-parser')
const assert          = require('assert');
const cassandra       = require('cassandra-driver');

// Express server (http://expressjs.com)
//###################################################################
var app = express();
app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())

var server = app.listen(8080, function () {
  var port = server.address().port;
  console.log("Service listening at port %s", port);
})

// Cassandra (http://cassandra.apache.org)
//###################################################################
const create_keyspace_directory  =
  "CREATE KEYSPACE IF NOT EXISTS directory WITH REPLICATION = { " +
  "'class'  : 'SimpleStrategy', " +
  "'replication_factor' : 1 }";

const create_table_signalcontext =
  "CREATE TABLE IF NOT EXISTS directory.signalcontext (" +
  "type text, " +
  "id text, " +
  "name text, " +
  "layer text, " +
  "owner text, " +
  "entrymap map<text, text>, " + // 'entries' is a reserved word
  "ts_write timestamp, " +
  "PRIMARY KEY ((type, id), layer, owner)) " +
  "WITH CLUSTERING ORDER BY (layer DESC)";

// Cassandra client (https://www.npmjs.com/package/cassandra-driver)
var client = new cassandra.Client(
  {contactPoints: ['127.0.0.1:9042']}
);

// connect to database
client.connect(function (err) {
  assert.ifError(err);
});
// create keyspace (if not exists)
client.execute(create_keyspace_directory, function (err){
  assert.ifError(err);
});
// use keyspace
client.execute("USE directory", function (err){
  assert.ifError(err);
});
// create table (if not exists)
client.execute(create_table_signalcontext, function (err, result) {
    assert.ifError(err);
});


// REST-API
//###################################################################
// get all owners and entries for id in given layer
app.get('/types/:type/ids/:id/layers/:layer',
function (request, response) {
  var type    = request.params.type;
  var id      = request.params.id;
  var layer   = request.params.layer;

  const query =
    "SELECT owner, entrymap "+
    "FROM directory.signalcontext "+
    "WHERE type = ? AND id = ? AND layer = ?";
  client.execute(query, [type, id, layer], { prepare: true },
    function (err, result) {
    if (err) {
        response.writeHead(204,
          {"Content-Type": "application/json"}
        );
        response.end(JSON.stringify({"error": err}));
    } else {
        var ownerDict = {};
        result.rows.forEach(function(row){
          ownerDict[row.owner]=row.entrymap;
        });
        response.writeHead(200,
          {"Content-Type": "application/json"}
        );
        response.end(JSON.stringify(ownerDict));
    }
  });
});

// get entries from multiple owners for id in given layer
app.put('/types/:type/ids/:id/layers/:layer/owners',
function (request, response) {
  var type     = request.params.type;
  var id       = request.params.id;
  var layer    = request.params.layer;
  var ownerSet = request.body.owners;

  var ownerEntryDict = {};
  const query =
  "SELECT owner, entrymap " +
  "FROM directory.signalcontext "+
  "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";

  if(ownerSet.length == 0){
    response.writeHead(200,
      {"Content-Type": "application/json"}
    );
    response.end(JSON.stringify(ownerEntryDict));
  }else{
    var count = 0;
    ownerSet.forEach(function(owner){
      client.execute(query, [type, id, layer, owner],
        { prepare: true }, function (err, result) {
        if (!err) {
          if(result.rows.length > 0){
            ownerEntryDict[result.rows[0].owner] =
              result.rows[0].entrymap;
          }
        }
        count++;
        if(count == ownerSet.length){
          response.writeHead(200,
            {"Content-Type": "application/json"}
          );
          response.end(JSON.stringify(ownerEntryDict));
        }
      });
    });
  }
});

// add entry for owner in certain layer
app.post('/types/:type/ids/:id/layers/:layer/owners/:owner/entries/:entryId',
function (request, response) {
  var type        = request.params.type;
  var id          = request.params.id;
  var layer       = request.params.layer;
  var owner       = request.params.owner;
  var entryId     = request.params.entryId;
  var entry       = request.body.entry;
  var name        = request.body.name;

  var input       = entryId+"':'"+entry;
  var currentTime = new Date().getTime();
  const query =
    "UPDATE directory.signalcontext " +
    "SET entrymap = entrymap + {'"+input+"'}, " +
    "name = '"+name+"', ts_write = "+currentTime+" " +
    "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";
  client.execute(query, [type, id, layer, owner],
    { prepare: true }, function (err) {
  if (err) {
      response.writeHead(500, {"Content-Type": "application/json"});
      response.end(JSON.stringify({"error": err}));
    } else {
      response.writeHead(200, {"Content-Type": "application/json"});
      response.end();
    }
  });
});

// remove entry from owner in certain layer
app.delete('/types/:type/ids/:id/layers/:layer/owners/:owner/entries/:entryId',
function (request, response) {
  var type        = request.params.type;
  var id          = request.params.id;
  var layer       = request.params.layer;
  var owner       = request.params.owner;
  var entryId     = request.params.entryId;

  const query =
    "UPDATE directory.signalcontext " +
    "SET entrymap = entrymap - {'"+entryId+"'} " +
    "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";
  client.execute(query, [type, id, layer, owner],
    { prepare: true }, function (err) {
    if (err) {
      response.writeHead(204, {"Content-Type": "application/json"});
      response.end(JSON.stringify({"error": err}));
    } else {
      response.writeHead(200, {"Content-Type": "application/json"});
      response.end();

      // manually remove owner without entries
      // cassandra removes whole database entry
      // automatically if no other owner exists!
      const query =
        "SELECT owner, entrymap " +
        "FROM directory.signalcontext " +
        "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";
      client.execute(query, [type, id, layer, owner],
        { prepare: true }, function (err, result) {
        if (err) {
          assert.ifError(err);
        } else {
          if(result["rows"][0]["entrymap"] == null){
            const query =
              "DELETE FROM directory.signalcontext " +
              "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";
            client.execute(query, [type, id, layer, owner],
              { prepare: true }, function (err) {
              assert.ifError(err);
            });
          }
        }
      });
    }
  });
});

// delete owner in certain layer
app.delete('/types/:type/ids/:id/layers/:layer/owners/:owner',
function (request, response) {
  var type     = request.params.type;
  var id       = request.params.id;
  var layer    = request.params.layer;
  var owner    = request.params.owner;

  const query =
    "DELETE FROM directory.signalcontext " +
    "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";
  client.execute(query, [type, id, layer, owner],
    { prepare: true }, function (err) {
    if (err) {
        response.writeHead(204, {"Content-Type": "application/json"});
        response.end(JSON.stringify({"error": err}));
    } else {
        response.writeHead(200, {"Content-Type": "application/json"});
        response.end();
    }
  });
});

// delete certain layer
app.delete('/types/:type/ids/:id/layers/:layer',
function (request, response) {
  var type     = request.params.type;
  var id       = request.params.id;
  var layer    = request.params.layer;

  const query =
    "DELETE FROM directory.signalcontext " +
    "WHERE type = ? AND id = ? AND layer = ?";
  client.execute(query, [type, id, layer],
    { prepare: true }, function (err) {
    if (err) {
        response.writeHead(204, {"Content-Type": "application/json" });
        response.end(JSON.stringify({"error": err}));
    } else {
        response.writeHead(200, {"Content-Type": "application/json"});
        response.end();
    }
  });
});

// get name of id set by owner in layer
app.get('/types/:type/ids/:id/layers/:layer/owners/:owner/name',
function (request, response) {
  var type     = request.params.type;
  var id       = request.params.id;
  var layer    = request.params.layer;
  var owner    = request.params.owner;

  const query =
    "SELECT name FROM directory.signalcontext " +
    "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";
  client.execute(query, [type, id, layer, owner],
    { prepare: true }, function (err, result) {
    if (!err && result.rows.length > 0) {
      response.writeHead(200, {"Content-Type": "application/json"});
      response.end(JSON.stringify({"name": result.rows[0].name}));
    } else {
      response.writeHead(500, {"Content-Type": "application/json"});
      response.end(JSON.stringify({"error": err}));
    }
  });
});

// update name of id by owner in layer
app.post('/types/:type/ids/:id/layers/:layer/owners/:owner/names/:name',
function (request, response) {
  var type     = request.params.type;
  var id       = request.params.id;
  var layer    = request.params.layer;
  var owner    = request.params.owner;
  var name     = request.params.name;

  var currentTime = new Date().getTime();
  const query =
    "UPDATE directory.signalcontext " +
    "SET name = '"+name+"', ts_write = "+currentTime+" " +
    "WHERE type = ? AND id = ? AND layer = ? AND owner = ?";
  client.execute(query, [type, id, layer, owner],
    { prepare: true }, function (err) {
    if (err) {
      response.writeHead(500, {"Content-Type": "application/json"});
      response.end(JSON.stringify({"error": err}));
    } else {
      response.writeHead(200, {"Content-Type": "application/json"});
      response.end();
    }
  });
});
