package Demo;

import com.twitter.heron.api.bolt.BaseRichBolt;
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

public class FinalBolt extends BaseRichBolt {
    private static final long serialVersionUID = -3226618846531432832L;
    private OutputCollector collector;
    private long startTime;
    private boolean emit;
    private AtomicInteger nItems;


    public FinalBolt(boolean emit) {
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
        try {
            nItems.getAndIncrement();
            Utils.sleep(1);
            long latency = System.currentTimeMillis() - startTime;
            System.out.println(tuple.getString(0) + " :: " + nItems.get() + " tuples in " + latency + " ms ");
            GlobalMetrics.incr("selected_items");
            if (emit) {
                collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            }
        } catch (Exception E){
            System.out.println("bolt execute error");
            System.out.println(E);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (emit) {
            declarer.declare(new Fields("word"));
        }
    }
}