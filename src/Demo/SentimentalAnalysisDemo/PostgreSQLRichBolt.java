package Demo.SentimentalAnalysisDemo;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhengyang on 2/11/17.
 */
public class PostgreSQLRichBolt extends BaseRichBolt {
    private OutputCollector collector;
    private long startTime;
    private AtomicInteger nItems;
    private Connection c = null;

    public PostgreSQLRichBolt() {
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
        try {
            c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
            c.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Statement stmt;
            nItems.getAndIncrement();
            long latency = System.currentTimeMillis() - startTime;
            System.out.println(nItems.get() + " tuples in " + latency + " ms " + "num:");
            stmt = c.createStatement();
            String sql = "INSERT INTO twittersentiment (twitter_handle,key,sentimentscore,timestamp) "
                    + "VALUES ('" + tuple.getString(0) + "', '"
                    + tuple.getString(2)+ "', '"
                    + Integer.parseInt(tuple.getString(1)) + "', '"
                    + System.currentTimeMillis()
                    + "');";
            stmt.executeUpdate(sql);
            stmt.close();
            c.commit();
            GlobalMetrics.incr("selected_items");
        } catch (Exception E){
            E.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
