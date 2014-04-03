package storm.starter;

import java.io.PrintWriter;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class TestTopology {
    @Option(name = "--name", usage = "Name for the topology")
    private static String topologyName = "TestTopology";

    @Option(name = "--numSpout", usage = "Number of spouts in the topology")
    private static int _numSpout = 1;

    @Option(name = "--ioRatio", usage = "Ratio of input to output in the bolts")
    private static double _inputOutputRatio = 1.0;

    @Option(name = "--children", usage = "Number of children each node has")
    private static int _children = 1;

    @Option(name = "--connection", usage = "Number of connections each children has")
    private static double _connection = 0;

    @Option(name = "--depth", usage = "How deep is the tree")
    private static int _depth = 1;

    @Option(name = "--numOutputBolt", usage = "Number of output bolts")
    private static int _numOutputBolt = 1;

    @Option(name = "--numTask", usage = "Number of tasks per bolt")
    private static int _numTask = 3;

    @Option(name = "--numWorker", usage = "Number of workers in Storm")
    private static int _numWorker = 3;

    private final String OUTPUT = "./topology.txt";

    public void run() throws Exception {
        PrintWriter writer = new PrintWriter(OUTPUT, "UTF-8");

        TopologyBuilder builder = new TopologyBuilder();

        /* Setting up spouts */
        for (int i = 0; i < _numSpout; i++) {
            builder.setSpout("spout_" + String.valueOf(i), new TestSpout(),
                _numTask);
        }

        /* Bolts (depth 0) connected to spout */
        for (int i = 0; i < _children; i++) {
            String boltName = "bolt_1_" + String.valueOf(i);
            BoltDeclarer bolt = builder.setBolt(boltName, new TestBolt(
                _inputOutputRatio), _numTask);

            for (int j = 0; j < _numSpout; j++) {
                String spoutName = "spout_" + String.valueOf(j);

                writer.println(spoutName + ',' + boltName);
                bolt.shuffleGrouping(spoutName);
            }
        }

        /* Bolts connected to previous bolts */
        for (int i = 2; i <= _depth; i++) {
            for (int j = 0; j < Math.pow(_children, i); j++) {
                String boltName = "bolt_" + String.valueOf(i) + "_"
                    + String.valueOf(j);
                BoltDeclarer bolt = builder.setBolt(boltName, new TestBolt(
                    _inputOutputRatio), _numTask);

                for (int k = 0; k < Math.pow(_children, i - 1); k++) {
                    String prevBoltName = "bolt_" + String.valueOf(i - 1) + "_"
                        + String.valueOf(k);

                    writer.println(prevBoltName + ',' + boltName);
                    bolt.shuffleGrouping(prevBoltName);
                }
            }
        }

        /* Output bolts */
        for (int i = 0; i < _numOutputBolt; i++) {
            String boltName = "output_bolt_" + String.valueOf(i);
            BoltDeclarer outputBolt = builder.setBolt(boltName, new TestBolt(
                _inputOutputRatio), _numTask);

            for (int j = 0; j < Math.pow(_children, _depth); j++) {
                String prevBoltName = "bolt_" + String.valueOf(_depth) + "_"
                    + String.valueOf(j);

                writer.println(prevBoltName + ',' + boltName);
                outputBolt.shuffleGrouping(prevBoltName);
            }
        }

        writer.close();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(_numWorker);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, _numWorker);

        StormSubmitter.submitTopology(topologyName, conf,
            builder.createTopology());
    }

    public static void main(String[] args) throws Exception {
        TestTopology topology = new TestTopology();
        CmdLineParser parser = new CmdLineParser(topology);

        try {
            parser.parseArgument(args);
            topology.run();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
