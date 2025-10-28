package flink.self.traning.statefulTransformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import flink.self.traning.models.Event;
import flink.self.traning.models.EventUserEnrichment;
import flink.self.traning.models.User;

public class EnrichmentWindowJoinApp {
    public static void main(String[] args) throws Exception {
        
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

        DataStream<EventUserEnrichment> enrichWindowJoin = eventSource.join(userSource)
                .where(event -> event.getUserId())
                .equalTo(user -> user.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .trigger(CountTrigger.of(1))
                .apply(
                        new JoinFunction<Event,User,EventUserEnrichment>() {
                            @Override
                            public EventUserEnrichment join(Event event, User user) throws Exception {
                                return new EventUserEnrichment(
                                    user.getId(),
                                    user.getName(),
                                    user.getGender(),
                                    user.getIsMembership(),
                                    event.getName(),
                                    event.getTimestamp()
                                );
                            }
                            
                        }
        );

        enrichWindowJoin.print("window join with process time");
        env.execute();

        
    }

}
