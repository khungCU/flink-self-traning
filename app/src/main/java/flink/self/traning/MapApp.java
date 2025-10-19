package flink.self.traning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.self.traning.models.Client;
import flink.self.traning.utils.MapClientName;

public class MapApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Client> ds = env.fromElements(
                new Client(1, "Ken", "male", true),
                new Client(2, "Joe", "male", true),
                new Client(3, "Alex", "female", true),
                new Client(4, "Kate", "female", false));

        // map with lambda expression
        // DataStream<Client> res = ds.map(client -> {
        //         String newName = client.getName().toUpperCase();
        //         client.setName(newName);
        //         return client;
        //     }
        // );

        // map with anonymous class
        // DataStream<Client> res = ds.map(new MapFunction<Client,Client>() {
        //     @Override
        //     public Client map(Client value) throws Exception {
        //         String newName = value.getName().toUpperCase();
        //         value.setName(newName);
        //         return value;}
        //     }  
        // );

        // map with custom class
        DataStream<Client> res = ds.map(new MapClientName());

        res.print();
        env.execute();
    }

}
