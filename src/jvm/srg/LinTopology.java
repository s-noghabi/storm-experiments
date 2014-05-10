package srg;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class LinTopology {
    private SrgConfig config;//= new SrgConfig("./topology.yaml");

    public void  run(String configFilePath, String topolgoyName) throws Exception{
        config = new SrgConfig(configFilePath);
        TopologyBuilder builder = new TopologyBuilder();
        String src_id= config.getNames()[0];
        builder.setSpout(src_id, new RandomSpout(), config.getNumTasks()[0]);

        for( int i=1; i<config.getNumTasks().length; i++) {
            String dst_id=config.getNames()[i];
            builder.setBolt(dst_id, new RandomBolt(config.getExecLatencies()[i]), config.getNumTasks()[i]).shuffleGrouping(src_id);
	    src_id= dst_id;
//            builder.setBolt("count", new RandomBolt(1), 12).fieldsGrouping("split", new Fields("word"));
        }
        Config conf = new Config();
        conf.setDebug(true);


        if (topolgoyName != null ) {
            conf.setNumWorkers(config.getNumWorker());

            StormSubmitter.submitTopology(topolgoyName, conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }

    }

    public static void main(String[] args) throws Exception {
        LinTopology topology= new LinTopology();
        topology.run(args[0], (args.length>1?args[1]:null));
    }
}
