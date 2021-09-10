package intrusionDetection;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class ConnectionSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean read = false;

    // Define the name of each column in the training data
    private final String[] field_names = new String[]{"duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in",
            "num_compromised", "root_shell", "su_attempted", "num_root", "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login", "count",
            "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
            "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
            "dst_host_rerror_rate", "dst_host_srv_rerror_rate"};


    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void nextTuple() {
        if (read) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }
        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try {

            while ((str = reader.readLine()) != null) {
                String[] fields = str.split(",");
                // Emit a tuple from input file
                this.collector.emit(new Values(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11],
                        fields[12], fields[13], fields[14], fields[15], fields[16], fields[17], fields[18], fields[19], fields[20], fields[21], fields[22], fields[23], fields[24],
                        fields[25], fields[26], fields[27], fields[28], fields[29], fields[30], fields[31], fields[32], fields[33], fields[34], fields[35], fields[36], fields[37],
                        fields[38], fields[39], fields[40]));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            read = true;
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Define the collector to be used for emitting tuples
        try {
            this.fileReader = new FileReader("kddcup_test_10.csv");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file");
        }
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare the name of each field of the tuples
        declarer.declare(new Fields(field_names[0], field_names[1], field_names[2], field_names[3], field_names[4], field_names[5], field_names[6], field_names[7], field_names[8], field_names[9],
                field_names[10], field_names[11], field_names[12], field_names[13], field_names[14], field_names[15], field_names[16], field_names[17], field_names[18], field_names[19], field_names[20],
                field_names[21], field_names[22], field_names[23], field_names[24], field_names[25], field_names[26], field_names[27], field_names[28], field_names[29], field_names[30], field_names[31],
                field_names[32], field_names[33], field_names[34], field_names[35], field_names[36], field_names[37], field_names[38], field_names[39], field_names[40]));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
