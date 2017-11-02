package Demo;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

/**
 * Created by zhengyang on 2/11/17.
 */
public class VaryingDistributionSprout  extends BaseRichSpout {
    private static final long serialVersionUID = 8397135974262200491L;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;
    int count;
    int MaxFreq = 40;

    public VaryingDistributionSprout() {
    }

    @SuppressWarnings("rawtypes")
    public void open(
            Map conf,
            TopologyContext context,
            SpoutOutputCollector acollector) {
        collector = acollector;
        rand = new Random();
        count = 10000;

    }

    public void close() {
    }

    public void nextTuple() {
        // We explicitly slow down the spout to avoid the stream mgr to be the bottleneck
        Utils.sleep(1);

        // 10000 - 8000
        if (count > 8000){
            words = new String[]{"a", "b", "c", "d", "e", "f", "g"};
            final String word = words[rand.nextInt(words.length)];
            // To enable acking, we need to emit tuple with MessageId, which is an object
            collector.emit(new Values(word), "MESSAGE_ID");
            System.out.println(count);
        }
        // 8000 - 6000
        else if (count > 6000){
            words = new String[]{"a", "a", "a","b" ,"b" ,"b" ,"b"  , "c", "d", "e", "f", "g"};
            final String word = words[rand.nextInt(words.length)];
            // To enable acking, we need to emit tuple with MessageId, which is an object
            collector.emit(new Values(word), "MESSAGE_ID");
            System.out.println(count);
        }
        // 6000 - 4000
        else if (count > 4000){
            words = new String[]{"a", "a" ,"b", "b", "b", "b", "b", "c", "d", "d" , "d" , "d" , "d" , "d" , "d", "e", "f", "g"};
            final String word = words[rand.nextInt(words.length)];
            // To enable acking, we need to emit tuple with MessageId, which is an object
            collector.emit(new Values(word), "MESSAGE_ID");
            System.out.println(count);
        }

        // 4000 - 2000
        else if (count > 2000){
            words = new String[]{"a", "a", "a", "a","b", "c", "d", "e", "f", "g"};
            final String word = words[rand.nextInt(words.length)];
            // To enable acking, we need to emit tuple with MessageId, which is an object
            collector.emit(new Values(word), "MESSAGE_ID");
            System.out.println(count);
        }
        else if (count > 0){
            words = new String[]{"a", "a", "b", "b", "c", "c", "c", "c", "d", "d", "d", "d", "e", "f", "f", "f", "f", "g"};
            final String word = words[rand.nextInt(words.length)];
            // To enable acking, we need to emit tuple with MessageId, which is an object
            collector.emit(new Values(word), "MESSAGE_ID");
            System.out.println(count);
        }
        count--;
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
