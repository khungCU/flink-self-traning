package flink.self.traning.statefulTransformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import flink.self.traning.models.Event;
import flink.self.traning.models.EventUserEnrichment;
import flink.self.traning.models.User;

public class EnrichmentSimpleApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Event> eventSource = env.fromElements(
            new Event("login", 1, 1000L),
            new Event("login", 2, 3000L),
            new Event("search", 1, 5000L),
            new Event("search", 1, 5000L),
            new Event("search", 1, 6000L),
            new Event("search", 2, 6000L),
            new Event("search", 1, 10000L)
        );

        DataStream<User> userSource = env.fromElements(
            new User(1, "Ken", "male", true),
            new User(2, "Alex", "female", false)
        );

        // Same code with Kafka/continuous streams: ❌ Dangerous - memory leak, will crash
        DataStream<EventUserEnrichment> joinedStream =  eventSource.join(userSource)
                                                                  .where(k -> k.getUserId())
                                                                  .equalTo(k -> k.getId())
                                                                  .window(GlobalWindows.create())
                                                                  .trigger(CountTrigger.of(1))  // Fire after each element
                                                                  .apply(
                                                                    new JoinFunction<Event, User, EventUserEnrichment>() {
                                                                        @Override
                                                                        public EventUserEnrichment join(Event left, User right)
                                                                                throws Exception {
                                                                            return new EventUserEnrichment(
                                                                                right.getId(),
                                                                                right.getName(),
                                                                                right.getGender(),
                                                                                right.getIsMembership(),

                                                                                left.getName(),
                                                                                left.getTimestamp()
                                                                            );
                                                                        }
                                                                        
                                                                    }
                                                                  );

        joinedStream.print("Enrichment Stream");
        env.execute();


    }
}

