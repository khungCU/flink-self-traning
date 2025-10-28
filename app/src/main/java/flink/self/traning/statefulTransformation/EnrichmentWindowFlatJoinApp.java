package flink.self.traning.statefulTransformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.self.traning.models.Event;
import flink.self.traning.models.User;

public class EnrichmentWindowFlatJoinApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> eventSource = env.fromElements(
            new Event("login", 1, 1000L),
            new Event("login", 2, 3000L),
            new Event("search", 1, 5000L),
            new Event("search", 1, 5000L),
            new Event("search", 1, 6000L),
            new Event("search", 2, 6000L),
            new Event("search", 2, 6000L),
            new Event("search", 1, 10000L),
             new Event("search", 2, 50000L)
        );

        DataStream<User> userSource = env.fromElements(
            new User(1, "Ken", "male", true),
            new User(2, "Alex", "female", false)
        );

        


    }

}
