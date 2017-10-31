package Demo.ThriftScalingDemo;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Map;
import java.util.Scanner;

/**
 * Created by zhengyang on 31/10/17.
 */
public class CnCBoltClient {

    private void invoke(Scanner sc) {
        TTransport transport;
        int i, result;
        try {
            transport = new TFramedTransport(new TSocket("localhost", 7911));
            TProtocol protocol = new TBinaryProtocol(transport);

            CnC.Client client = new CnC.Client(protocol);
            transport.open();
            while (true){
                System.out.println("Please enter 1 to scaleUp, 2 to scaleDown, 3 to print state " +
                        "any other value to quit");
                i = sc.nextInt();
                if (i == 1){
                    System.out.println("Please enter amt to scaleUp");
                    result = client.scaleup(sc.nextInt());
                    System.out.println("NumCore :: " + result);
                } else if (i == 2){
                    System.out.println("Please enter amt to scaleDown");
                    result = client.scaledown(sc.nextInt());
                    System.out.println("NumCore :: " + result);
                } else if (i == 3){
                    Map<String, Integer> map = client.getState();
                    System.out.println("Printing State: ");
                    for (String key: map.keySet()) {
                        System.out.println(key + ": " + map.get(key));
                    }
                } else {
                    break;
                }
            }
            transport.close();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        Scanner sc;
        sc = new Scanner(System.in);
        CnCBoltClient c = new CnCBoltClient();
        c.invoke(sc);
    }
}