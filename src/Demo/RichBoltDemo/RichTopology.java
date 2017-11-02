package Demo.RichBoltDemo;

import Demo.FinalBolt;
import Demo.VaryingNumKeySprout;
import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;

/**
 * Created by zhengyang on 27/10/17.
 */
public class RichTopology {
    private RichTopology() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Please specify the name of the topology");
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new VaryingNumKeySprout(), 2);
        // (name, elastic bolt instance, number of cores used)
        builder.setBolt("exclaim1", new RichBolt(true), 4)
                .fieldsGrouping("word", new Fields("word"));
        builder.setBolt("final", new FinalBolt(false), 1)
                .fieldsGrouping("exclaim1", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);


        // Put an arbitrary large number here if you don't want to slow the topology down
        conf.setMaxSpoutPending(1000 * 1000 * 1000);

        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

        // Set number of containers
        conf.setNumStmgrs(1);

        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
