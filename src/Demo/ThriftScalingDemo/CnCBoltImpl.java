package Demo.ThriftScalingDemo;

/**
 * Created by zhengyang on 31/10/17.
 */

import com.twitter.heron.api.bolt.IElasticBolt;
import org.apache.thrift.TException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhengyang on 29/9/17.
 */
public class CnCBoltImpl implements CnC.Iface {
    private IElasticBolt server;

    public CnCBoltImpl(IElasticBolt server) {
        this.server = server;
    }

    @Override
    public int scaleup(int amt) throws TException {
        System.out.println("Scaling up by " + amt);
        server.scaleUp(amt);
        return server.getNumCore();
    }

    @Override
    public int scaledown(int amt) throws TException {
        System.out.println("Scaling down by " + amt);
        server.scaleDown(amt);
        return server.getNumCore();
    }

    @Override
    public ConcurrentHashMap<String,Integer> getState() throws TException {
        return server.getStateMap();
    }
}