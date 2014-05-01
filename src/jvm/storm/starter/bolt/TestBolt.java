package storm.starter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestBolt extends BaseRichBolt {
    private final double _inputOutputRatio;
    private OutputCollector _collector;

    public TestBolt(double inputOutputRatio) {
        _inputOutputRatio = inputOutputRatio;
    }

    @Override
    public void prepare(Map conf, TopologyContext context,
        OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);

        if (_inputOutputRatio > 1) {
            for (int i = 0; i < _inputOutputRatio; i++) {
                _collector.emit(new Values(word));
                _collector.ack(tuple);
            }
        } else {
            if (Math.random() < _inputOutputRatio) {
                _collector.emit(new Values(word));
                _collector.ack(tuple);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
