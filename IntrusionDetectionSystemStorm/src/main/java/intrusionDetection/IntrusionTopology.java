package intrusionDetection;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class IntrusionTopology {

    public static void main(String[] args) {
        // Build and submit the topology to a cluster
        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new ConnectionSpout(), 2);
        builder.setBolt("process", new DataPreprocessingBolt(), 8).shuffleGrouping("spout");
        builder.setBolt("model", new PredictionModelBolt(), 8).shuffleGrouping("process");

        conf.setNumWorkers(20);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("IntrusionDetection", conf, builder.createTopology());

        // Stop
        Utils.sleep(600000);
        cluster.killTopology("IntrusionDetection");
        cluster.shutdown();

    }
}
