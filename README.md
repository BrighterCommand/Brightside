# Brightside
Brightside is a command dispatcher and processor for Python. It also provides a work queue for asynchronous dispatch over a broker such as RabbitMQ.
It is intended to work with Brighter, a C# command dispatcher and processor and allow polyglot solutions i.e. a Flask endpoint that sends work over a queue to a C# consumer or an ASP.NET WebAPI endpoint that sends a request over a work queue to a Python application.
Brightside can also be used stand-alone as an opinionated alternative to libraries like Celery, which use RPC and not messaging.


## Message Store Configuration
If you post messages via a Broker, and use alchemy_store then We need to know where the tables for the message store can be found;
we don't create the database that contains them, we assume that you will do that, and so we need a connection string to find that
database. To do this, we pick up the following environment variable:

BRIGHTER_MESSAGE_STORE_URL

If this environment variable is not set, we will generate an error

## Docker Compose File
The Docker Compose File is intended to provide sufficient infrastructure for you to run tests that require backing stores or Message Oriented Middleware.
The testrunner.py script finds all test fixtures with the naming convention tests_* and runs them
To run the tests just use ./run_rests.sh
This shell script will docker-compose up the required infrastructure and a container for Brightside code and tests; that container is kept running with top
Once running we use docker-exec to run the python test runner script

[![Build Status](https://travis-ci.org/BrighterCommand/Brightside.svg?branch=master)](https://travis-ci.org/BrighterCommand/Brightside)



