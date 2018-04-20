# Release Notes

Packages are available on [PyPI](https://pypi.org/project/brightside/)

You can use the run_tests.sh file to run the test suite via docker. The script uses the tests-docker-compose.yml file to provide the dependencies for the tests, deploys the code and then runs the test suite.

## Master

## Release 0.6.4
-- Fixed issues produced during partition testing

Assume I have a cluster with three nodes: A,B, C

Assume that I have a queue, orders, with the master on A and slaves on B and C.

We have chosen a strategy of Pause Minority on a partition

Assume I connect to Node B. I consume from the master on A via Node B. (I don't consume from the slave, that is there in case A fails). Then B gets a partition and I cannot see A or C. I need to re-connect to A via A or C. I have chosen an Pause Minority strategy.
    * RMQ will pause the partioned node
    * We will timeout on our connection
    * I need to stop talking to B and talk to A or C.

    ###### Results
    * Consumer is Idle
        * RMQ reports that only one mirror now exists
        * Socket error on Brightside client
        * Consumer connects to new node ater delay
    * Consumer is Busy
        * RMQ reports that only one mirror now exists
        * Socket error on Brightside client
        * Consumer connects to new node ater delay
            * Does not seem to consume the messages from the queue -- appears to be a zombie
            * Fixed - handle socket errors by recreating connection
            
-- Fixed issue that older versions of RabbitMQ do not use policy and require the x-hapolicy flag to be set at queue creation.
