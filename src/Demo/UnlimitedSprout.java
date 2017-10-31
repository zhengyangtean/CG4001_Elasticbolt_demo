package Demo;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by zhengyang on 31/10/17.
 */
public class UnlimitedSprout extends BaseRichSpout {
    private static final long serialVersionUID = 8887423104076121301L;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;

    public UnlimitedSprout() {
    }

    @SuppressWarnings("rawtypes")
    public void open(
            Map conf,
            TopologyContext context,
            SpoutOutputCollector acollector) {
        collector = acollector;
        words = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"};
        rand = new Random();
    }

    public void close() {
    }

    public void nextTuple() {
        // We explicitly slow down the spout to avoid the stream mgr to be the bottleneck
        Utils.sleep(1);
        final String word = words[rand.nextInt(words.length)];
        // To enable acking, we need to emit tuple with MessageId, which is an object
        collector.emit(new Values(word), "MESSAGE_ID");
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}