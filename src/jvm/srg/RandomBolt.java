package srg;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class RandomBolt extends BaseRichBolt {
//    private final double _inputOutputRatio;
    private final double _execute_time;
    private OutputCollector _collector;
    private BufferedWriter bw;
    public RandomBolt( double _execute_time) {

//        _inputOutputRatio = inputOutputRatio;
        this._execute_time=_execute_time*20; //TODO: change 1000
    }

    @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {

        _collector = collector;
        try {
            File file= new File("testfile.txt");
            if(!file.exists()){
                file.createNewFile();
            }
            bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
        }catch(Exception e){
            System.out.println("error in reading file");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        for(int i=0; i<_execute_time; i++){
            for( int j=0; j<_execute_time; j++){
                try {
                    bw.append(""+i);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        _collector.emit(new Values(str));

       /* if (_inputOutputRatio > 1) {
            for (int i = 0; i < _inputOutputRatio; i++) {
                _collector.emit(new Values(word));
                // _collector.ack(tuple);
            }
        } else {
            if (Math.random() < _inputOutputRatio) {
                _collector.emit(new Values(word));
                // _collector.ack(tuple);
            }
        }
*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
