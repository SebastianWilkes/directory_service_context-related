# directory_service_context-related
A directory service for exchanging context-related information by using IDs from beacons and other sources.

This README explains the installation and usage of the directory service.

### How do I get set up? ###

**install node.js**
https://nodejs.org/en/download/

**install packages**
> npm install express 

> npm install body-parser

> npm install assert 

> npm install cassandra-driver

**install cassandra**
http://cassandra.apache.org

For MacOS:

install brew (https://docs.brew.sh/Installation)

> ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

> brew update

> brew install cassandra

> brew services start cassandra 

(wait a minute.. then start the shell)

> cqlsh

**start directory service**
> node app.js

(error? make sure cassandra is running)

### How to run tests? ###

Test via Cassandra shell (http://cassandra.apache.org/doc/latest/tools/cqlsh.html)
use SQL-like commands like: 

> SELECT * FROM directory.signalcontext;

> SELECT * FROM directory.signalcontext WHERE layer = 'tests';

Test via Postman -> https://www.getpostman.com

import collection of API requests: postman/api-testing_local.postman_collection.json (localhost)

import collection of API requests: postman/api-testing.postman_collection.json (deployed service)

### Questions? ###

sebastian.wilkes@haw-hamburg.de

sebastianwilkes@gmail.com (private)
