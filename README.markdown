# Compiling

    $ ./scripts/compile.sh

# Running experiments

    $ ./scripts/run.sh TestTopology <options>

The following options are available.
* --name=<Name of Topology>
* --num-spout=<number of spout>
* --io-ratio=<Ratio if data coming in/out to/of bolt>
* --children=<Number of children each bolt has>
* --depth=<Depth of the topology>

This project is being adapted from https://github.com/nathanmarz/storm-starter
