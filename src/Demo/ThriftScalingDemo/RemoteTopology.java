package Demo.ThriftScalingDemo;

import Demo.FinalBolt;
import Demo.UnlimitedSprout;
import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;

/**
 * Created by zhengyang on 27/10/17.
 */
public class RemoteTopology {
    private RemoteTopology() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Please specify the name of the topology");
        }
        int parallelism = 1;
        int numThreads = 3;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new UnlimitedSprout(), 2);
        builder.setBolt("exclaim1", new RemoteBolt(true), parallelism, numThreads, true, 1)
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
