package Demo.SentimentalAnalysisDemo;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;

/**
 * Created by zhengyang on 2/11/17.
 */

public class SentimentalTopology {

    private SentimentalTopology() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Please specify the name of the topology");
        }

        int parallelism = 1;
        int numThreads = 4;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweetStreamSprout",
                new TweetSprout(new String[]{"trump", "potus", "realDonaldTrump"}), 1);
        builder.setBolt("sentimental", new SentimentalElasticBolt(true),  parallelism, numThreads, true, 1)
                .fieldsGrouping("tweetStreamSprout", new Fields("Author", "Tweet", "SearchKey"));
        builder.setBolt("exclaim", new PostgreSQLRichBolt(), 1)
                .fieldsGrouping("sentimental", new Fields("Author", "Sentiment", "SearchKey" ));
        Config conf = new Config();
        conf.setDebug(true);
        // Put an arbitrary large number here if you don't want to slow the topology down
        conf.setMaxSpoutPending(1000 * 1000 * 1000);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
        conf.setNumStmgrs(1); // Set number of containers
        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}