package Demo.SentimentalAnalysisDemo;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

/**
 * Created by zhengyang on 2/11/17.
 */
public class modifiedListener implements StatusListener {
    TweetSprout parent;

    public modifiedListener(TweetSprout parentProcess){
        parent = parentProcess;
    }

    @Override
    public void onStatus(Status status) {

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

    }
}
