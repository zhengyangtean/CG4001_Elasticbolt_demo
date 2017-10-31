package Demo.ThriftScalingDemo;

import com.twitter.heron.api.bolt.BaseElasticBolt;
import com.twitter.heron.api.bolt.IElasticBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * Created by zhengyang on 31/10/17.
 */
public class RemoteBolt extends BaseElasticBolt{
    private static final long serialVersionUID = 2465752871851918150L;
    private OutputCollector collector;
    private long startTime;
    private boolean emit;
    private AtomicInteger nItems;
    private Socket smtpSocket = null;
    private DataOutputStream os = null;
    private HttpClient httpclient;
    private HttpPost httppost;
    private String URL = "http://localhost:5000/postData";

    public RemoteBolt(boolean emit) {
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
        RemoteBolt.Server svr = new RemoteBolt.Server(this);
        svr.start();
        try {
            smtpSocket = new Socket("localhost", 50009);
            os = new DataOutputStream(smtpSocket.getOutputStream());
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host: hostname");
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to: hostname");
        }

        httpclient = HttpClients.createDefault();

        httppost = new HttpPost(URL);
    }

    @Override
    public void cleanup() {
        try {
            if (os != null) {
                os.close();
            }
            if (smtpSocket != null){
                smtpSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("KILLING SOCKET!");
        super.cleanup();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Utils.sleep(100);
            nItems.getAndIncrement();
            long latency = System.currentTimeMillis() - startTime;
            GlobalMetrics.incr("selected_items");
            System.out.println(tuple.getString(0) + " :: " +nItems.get() + " in " + latency
                    + " num/max: " + getNumCore() + "/" + getMaxCore() + "/" + getNumOutStanding() + " :: " + getNumWorkingKeys());
            this.incrementAndGetState(tuple.getString(0),1);

            if (emit) {
                emitTuple(tuple, new Values(tuple.getString(0)+ "!!",Integer.toString(getNumCore())));
            }
        } catch (Exception E){
            System.out.println("bolt execute error");
            System.out.println(E);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (emit) {
            declarer.declare(new Fields("word", "numcore"));
        }
    }

    @Override
    public void runBoltHook() {
        // Posting data to DisplayServer
        List<NameValuePair> params = new ArrayList<>(3);
        params.add(new BasicNameValuePair("time", Long.toString(System.currentTimeMillis())));
        params.add(new BasicNameValuePair("inqueueSize", Integer.toString(inQueue.size())));
        params.add(new BasicNameValuePair("numCore", Integer.toString(getNumCore())));
        try {
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            httpclient.execute(httppost);
            httppost.releaseConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public class Server extends Thread {
        IElasticBolt srv;

        public Server(IElasticBolt srv){
            this.srv = srv;
        }

        public void run() {
            try {
                TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(7911);
                CnC.Processor processor = new CnC.Processor(new CnCBoltImpl(srv));
                TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                        processor(processor));
                System.out.println("Starting server on port 7911 ...");
                server.serve();
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }
}
