package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestBolt extends BaseBasicBolt {
	private final double _inputOutputRatio;

	public TestBolt(double inputOutputRatio) {
		_inputOutputRatio = inputOutputRatio;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getString(0);

		if (_inputOutputRatio > 1) {
			for (int i = 0; i < _inputOutputRatio; i++) {
				collector.emit(new Values(word));
			}
		} else {
			if (Math.random() < _inputOutputRatio) {
				collector.emit(new Values(word));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
