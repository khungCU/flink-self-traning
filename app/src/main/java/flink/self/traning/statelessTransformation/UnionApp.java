package flink.self.traning.statelessTransformation;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import flink.self.traning.models.Client;
import java.util.Collection;
import java.util.List;

/*
 * Union method is to merge two same type of the dataStream 
 * and output the same type of the dataStream
 */


public class UnionApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Collection<Client> dummyClientGroupA = List.of(
                new Client(1, "Ken1", "male", true),
                new Client(2, "Joe1", "male", true),
                new Client(3, "Alex1", "female", true),
                new Client(4, "Kate1", "female", false)
        );

         Collection<Client> dummyClientGroupB = List.of(
                new Client(5, "Ken2", "male", true),
                new Client(6, "Joe2", "male", true),
                new Client(7, "Alex2", "female", true),
                new Client(8, "Kate2", "female", false)
        );

        DataStream<Client> ds1 = env.fromCollection(dummyClientGroupA);
        DataStream<Client> ds2 = env.fromCollection(dummyClientGroupB);
        DataStream<Client> res = ds1.union(ds2);

        res.print();
        env.execute();
    }

}
