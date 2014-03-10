# Compiling

    $ ./scripts/compile.sh

# Running experiments

    $ ./scripts/run.sh <options>

The following options are available.
* --name=<Name of Topology>
* --numSpout=<number of spout>
* --ioRatio=<Ratio if data coming in/out to/of bolt>
* --children=<Number of children each bolt has>
* --depth=<Depth of the topology>
* --numOutputBolt=<Number of output bolts>
* --numTask=<Number of task per bolt>

This project is being adapted from https://github.com/nathanmarz/storm-starter
