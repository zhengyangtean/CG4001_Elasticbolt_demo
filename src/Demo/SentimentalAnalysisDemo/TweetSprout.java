package Demo.SentimentalAnalysisDemo;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.common.basics.Pair;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.LinkedList;
import java.util.Map;

/**
 * Created by zhengyang on 2/11/17.
 */
public class TweetSprout extends BaseRichSpout {
    private static final long serialVersionUID = -4135870137180833969L;
    private SpoutOutputCollector collector;
    public String[] keywords;
    TwitterStream twitterStream;
    int count;
    public LinkedList<Pair<String, String>> buffer;
    Twitter twitter;

    public TweetSprout(String[] keywords) {
        this.keywords = keywords;
    }

    @SuppressWarnings("rawtypes")
    public void open(
            Map conf,
            TopologyContext context,
            SpoutOutputCollector acollector) {
        collector = acollector;

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(System.getenv("TWITTER_CONSUMER_KEY"))
                .setOAuthConsumerSecret(System.getenv("TWITTER_CONSUMER_SECRET"))
                .setOAuthAccessToken(System.getenv("TWITTER_ACCESS_TOKEN"))
                .setOAuthAccessTokenSecret(System.getenv("TWITTER_ACCESS_TOKEN_SECRET"));

        twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        buffer = new LinkedList<>();
        count = 0;

        modifiedListener listener = new modifiedListener(this) {
            @Override
            public void onStatus(Status status) {
                parent.buffer.add(new Pair<>( status.getUser().getScreenName(),status.getText()));
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        FilterQuery tweetFilterQuery = new FilterQuery();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

    public void close() {
    }

    public void nextTuple() {

        // We explicitly slow down the spout to avoid the stream mgr to be the bottleneck
        while (buffer.size() == 0) {
            System.out.println("waiting for tuple");
        }
        while (buffer.size() > 0){
            Utils.sleep(1);
            System.out.println(++count + " Tuples");
            Pair<String, String> temp = buffer.poll();
            if (temp.first != null && temp.second != null){
                collector.emit(new Values(temp.first, temp.second, keywords[0]), "MESSAGE_ID");
            }
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Author", "Tweet","SearchKey"));
    }
}
