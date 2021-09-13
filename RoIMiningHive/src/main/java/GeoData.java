import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class GeoData extends UDF {
    HashMap<String, HashSet<String>> keyWords;
    String fileName = "hdfs://hostname:port/KeywordFiles//";

    public GeoData() {
        super();
        keyWords = loadKeyWord(fileName);
    }

    private HashMap<String, HashSet<String>> loadKeyWord(String fileName) {
        // Read keyword files
        File root = new File(fileName);
        File[] files = root.listFiles();
        HashMap<String, HashSet<String>> keywords = new HashMap<>();
        Properties prop = new Properties();
        FileInputStream fileInput;
        String name;
        try {
            if (files != null) {
                for (File f : files) {
                    name = null;
                    fileInput = new FileInputStream(f.getPath());
                    prop.load(fileInput);
                    if (prop.getProperty("name").length() > 0) {
                        name = prop.getProperty("name");
                    }
                    String[] keys = prop.getProperty("poiKeysS").split(",");
                    keywords.put(name, new HashSet<>(Arrays.asList(keys)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keywords;
    }

    public Text evaluate(String tags, String description) {
        // Assign the name of a RoI by comparing tags and description with keywords
        String roiName = assignRoI(tags, description);
        if (roiName != null) return new Text(roiName);
        else return null;
    }

    private String assignRoI(String tags, String description) {
        //Checks if the tags and description contain one of the keywords contained in the keyWords HashSet of a particular RoI,
        // If so, return the respective key representing the name of the RoI
        return null;
    }
}