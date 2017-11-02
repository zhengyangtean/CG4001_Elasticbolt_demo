package Demo.SentimentalAnalysisDemo;

import com.twitter.heron.api.bolt.BaseElasticBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhengyang on 2/11/17.
 */
public class SentimentalElasticBolt extends BaseElasticBolt {
    private static final long serialVersionUID = -1783158526609571915L;
    private OutputCollector collector;
    private long startTime;
    private boolean emit;
    private AtomicInteger nItems;
    StanfordCoreNLP pipeline;
    int mainSentiment;

    public SentimentalElasticBolt(boolean emit) {
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
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            mainSentiment = 0;
            String tweet = tuple.getString(1);
            if (tweet != null && tweet.length() > 0) {
                int longest = 0;
                Annotation annotation = pipeline.process(tweet);
                for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                    int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                    String partText = sentence.toString();
                    if (partText.length() > longest) {
                        mainSentiment = sentiment;
                        longest = partText.length();
                    }
                }
            }
            GlobalMetrics.incr("selected_items");
            nItems.getAndIncrement();
            long latency = System.currentTimeMillis() - startTime;
            // usages of ElasticBolt's builtin state
            int weightedAverage = getState(tuple.getString(0), -1); // -1 to indicate that recorded sentiment for user
            if (weightedAverage == -1){
                putState(tuple.getString(0),mainSentiment);
            } else { // if there is an weighted average recorded before
                putState(tuple.getString(0), (int)Math.ceil((mainSentiment + weightedAverage) / 2.0)); // update new weighted avg
            }
            System.out.println("Handle :: " + tuple.getString(0) + ", currentSentiment :: "
                    + Integer.toString(mainSentiment) + ", avgSentiment :: " + getState(tuple.getString(0)) + ", "
                    + nItems.get() + " tuples in " + latency + " ms " + "num:" + getNumCore() + "^" + getMaxCore());

            if (emit) {
                emitTuple(tuple, new Values(tuple.getString(0), Integer.toString(mainSentiment), tuple.getString(2)));
            }
        } catch (Exception E){
            System.out.println("bolt execute error");
            System.out.println(E);
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (emit) {
            declarer.declare(new Fields("Author", "Sentiment", "SearchKey"));
        }
    }
}