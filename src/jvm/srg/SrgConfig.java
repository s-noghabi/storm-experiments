package srg;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Vector;

/**
 * Created by abdolla2 on 5/2/14.
 */
public class SrgConfig {
    private int numWorker;
    private int numTasks[]; //all bolts and spout in order
    private double execLatencies [];//={0,2,3};
    private String names[];//={"spout","b1","b2"};
    private String TOPOLOGY_FILE = "./topology.txt";
    private String EXECUTOR_FILE = "./executor.txt";

    public static void main(String[] args) {
        SrgConfig config = new SrgConfig("./topology.yaml");
    }
    public SrgConfig(String path){
        this.readYaml(path);
    }
    private void readYaml(String path) {
        File configFile = new File(path);
        Yaml yaml = new Yaml();
        try {
            Map map = (Map) yaml.load(new FileInputStream(configFile));
            numWorker = (Integer) map.get("numWorkers");
//            String topologyName = (String) map.get("topolgoyName");
            Map<String, Map<String, Integer>> elements = (Map) map.get("elements");
            names = new String[elements.size()];
            numTasks = new int[elements.size()];
            execLatencies = new double[elements.size()];

//            names = (String[]) elements.keySet().toArray();
            for (int i = 0; i < elements.size(); i++) {
                names[i] = (String) elements.keySet().toArray()[i];
                Map<String, Integer> element = elements.get(names[i]);
                numTasks[i] = element.get("tasks");
                execLatencies[i] = element.get("executeLatency");
            }
            System.out.println(elements);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public double[] getExecLatencies() {
        return execLatencies;
    }

    public int getNumWorker() {
        return numWorker;
    }

    public int[] getNumTasks() {
        return numTasks;
    }

    public String[] getNames() {
        return names;
    }

    public String getEXECUTOR_FILE() {
        return EXECUTOR_FILE;
    }

    public String getTOPOLOGY_FILE() {
        return TOPOLOGY_FILE;
    }
}
