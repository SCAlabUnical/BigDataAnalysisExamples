package intrusionDetection;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class DataPreprocessingBolt extends ShellBolt implements IRichBolt {

    private final String[] field_names = new String[]{"duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in",
            "num_compromised", "root_shell", "su_attempted", "num_root", "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login", "count",
            "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
            "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
            "dst_host_rerror_rate", "dst_host_srv_rerror_rate"};

    public DataPreprocessingBolt() {
        // Use the Multi-Language protocol to run a Python script
        super("python3", "/home/user/IdeaProjects/ApacheStorm/src/main/resources/preprocessingBolt.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
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
