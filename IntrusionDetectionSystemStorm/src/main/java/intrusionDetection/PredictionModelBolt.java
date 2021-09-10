package intrusionDetection;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class PredictionModelBolt extends ShellBolt implements IRichBolt {

    public PredictionModelBolt() {
        // Use the Multi-Language protocol to run a Python script
        super("python3", "/home/user/IdeaProjects/ApacheStorm/src/main/resources/modelBolt.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("prediction"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
