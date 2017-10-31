package Demo.FFDElasticBoltDemo;

import com.twitter.heron.api.bolt.FFDElasticBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhengyang on 31/10/17.
 */
public class FFDElasticExclaimationBolt extends FFDElasticBolt {
    private static final long serialVersionUID = 1936797944812719776L;
    private OutputCollector collector;
    private long startTime;
    private boolean emit;
    private AtomicInteger nItems;

    public FFDElasticExclaimationBolt(boolean emit) {
        this.emit = emit;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(
            Map conf,
            TopologyContext context,
            OutputCollector acollector) {
        collector = acollector;
        nItems = new AtomicInteger(0);
        startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        Utils.sleep(10);
        nItems.getAndIncrement();
        long latency = System.currentTimeMillis() - startTime;
        GlobalMetrics.incr("selected_items");
        System.out.println(tuple.getString(0) + " :: " +nItems.get() + " in " + latency
                + " num/max: " + getNumCore() + "/" + getMaxCore() + "/" + getNumOutStanding());
        if (emit) {
            emitTuple(tuple, new Values(tuple.getString(0) + "!!!"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (emit) {
            declarer.declare(new Fields("word"));
        }
    }
}
