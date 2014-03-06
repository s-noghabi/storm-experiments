package storm.starter;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class TestTopology {
	@Option(name = "--name", usage = "Name for the topology")
	private static String topologyName = "TestTopology";

	@Option(name = "--num-spout", usage = "Number of spouts in the topology")
	private static int _numSpout = 1;

	@Option(name = "--io-ratio", usage = "Ratio of input to output in the bolts")
	private static double _inputOutputRatio = 1.0;

	@Option(name = "--children", usage = "Number of children each node has")
	private static int _children = 1;

	@Option(name = "--connection", usage = "Number of connections each children has")
	private static double _connection = 0;

	@Option(name = "--depth", usage = "How deep is the tree")
	private static int _depth = 1;

	public void run() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		/* Setting up spouts */
		for (int i = 0; i < _numSpout; i++) {
			builder.setSpout("spout " + String.valueOf(i), new TestSpout(), 3);
		}

		/* Bolts (depth 0) connected to spout */
		for (int i = 0; i < _children; i++) {
			String boltName = "bolt 0 " + String.valueOf(i);
			BoltDeclarer bolt = builder.setBolt(boltName, new TestBolt(
					_inputOutputRatio), 3);

			for (int j = 0; j < _numSpout; j++) {
				String spoutName = "spout " + String.valueOf(j);
				bolt.shuffleGrouping(spoutName);
			}
		}

		/* Bolts connected to previous bolts */
		for (int i = 1; i < _depth; i++) {
			for (int j = 0; j < _children; j++) {
				String boltName = "bolt " + String.valueOf(i) + " "
						+ String.valueOf(j);

				BoltDeclarer bolt = builder.setBolt(boltName, new TestBolt(
						_inputOutputRatio), 3);

				for (int k = 0; k < _children; k++) {
					String prevBoltName = "bolt " + String.valueOf(i) + " "
							+ String.valueOf(k);

					bolt.shuffleGrouping(prevBoltName);
				}
			}
		}

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(3);

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
