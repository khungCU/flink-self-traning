package flink.self.traning;

import java.util.stream.Stream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import flink.self.traning.models.Client;
import flink.self.traning.models.User;

public class ConnectApp {
    public static void main(String[] args) throws Exception {

        // using connect() method to connect two stream with different type together

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Client> dsc = env.fromElements(
                new Client(1, "Ken", "male", true),
                new Client(2, "Joe", "male", false),
                new Client(3, "Alex", "female", true),
                new Client(4, "Kate", "female", false)
        );

        DataStream<User> dsu = env.fromElements(
            new User(1, "Ken", "male", true),
            new User(2, "Joe", "male", true),
            new User(4, "Kate", "female", false)   
        );

        // Step 1 : Merge the stream into one ConnectedStreams 
        ConnectedStreams<Client, User> conS = dsc.connect(dsu);

        // Step 2: Process element from two streams and transform into a single unified output stream
        DataStream<String> res = conS.map(
            new CoMapFunction<Client,User,String>() {
                // Both map1 and map2 must return the same type
                @Override
                public String map1(Client value) throws Exception {
                    // processes elements from the first stream (Client)
                    return value.getName() + " is a Client" ;
                }

                @Override
                public String map2(User value) throws Exception {
                    // processes elements from the second stream (User)
                    return value.getName() + " is an User";
                }
                
            }
        );
        res.print();

        env.execute();
         
        
    }

}
