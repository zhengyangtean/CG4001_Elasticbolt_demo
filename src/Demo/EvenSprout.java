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
 * Created by zhengyang on 2/11/17.
 */
public class EvenSprout extends BaseRichSpout {
    private static final long serialVersionUID = -1456953103414653481L;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;
    int count;
    int currLetter;

    public EvenSprout() {
    }

    @SuppressWarnings("rawtypes")
    public void open(
            Map conf,
            TopologyContext context,
            SpoutOutputCollector acollector) {
        collector = acollector;
        words = new String[]{"a", "b", "c", "d", "e", "f"};
        currLetter = 0;
        rand = new Random();
        count = 10000;
    }

    public void close() {
    }

    public void nextTuple() {
        // We explicitly slow down the spout to avoid the stream mgr to be the bottleneck
        Utils.sleep(1);
        if (count-- > 0){
            final String word = words[currLetter];
            currLetter += 1;
            currLetter %= words.length;
            // To enable acking, we need to emit tuple with MessageId, which is an object
            collector.emit(new Values(word), "MESSAGE_ID");
            System.out.println(word);
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}