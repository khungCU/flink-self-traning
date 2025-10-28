package flink.self.traning.statefulTransformation;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import flink.self.traning.models.Event;
import flink.self.traning.models.EventUserEnrichment;
import flink.self.traning.models.User;

public class EnrichmentMemoryStateLookupJoinApp {
    public static void main(String[] args) throws Exception {
        /*
         * As Stream world the orders are not ganrentee here you have to 
         * run couple of times as we can't control which source ingest first
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<User> userSource = env.fromElements(
            new User(1, "Ken", "male", true),
            new User(2, "Alex", "female", false)
        );
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

        DataStream<EventUserEnrichment> enrichedStream = eventSource
            .connect(userSource)
            .keyBy(
                event -> event.getUserId(),
                user -> user.getId()
            )
            .process(new CoProcessFunction<Event,User,EventUserEnrichment>() {
                //  Storage for lookup (Flink managed state)
                private MapState<Integer, User> userMap;
                

                @Override
                public void open(Configuration parameters) throws Exception {
                    // Initialize user state
                    MapStateDescriptor<Integer, User> userDescriptor =
                        new MapStateDescriptor<>(
                            "userState",    // state name
                            Integer.class,  // key type (userId)
                            User.class      // value type (User object)
                        );
                    userMap = getRuntimeContext().getMapState(userDescriptor);
                }

                @Override
                public void processElement1(Event event, Context ctx, Collector<EventUserEnrichment> out) throws Exception {
                    // Processes elements from first stream (Events)
                    // Step 1 : look up User exists in the state
                    User user = userMap.get(event.getUserId());

                    // Step 2: emit enriched result if found
                    if (user != null) {
                        out.collect(
                        new EventUserEnrichment(
                            user.getId(),
                            user.getName(),
                            user.getGender(),
                            user.getIsMembership(),
                            event.getName(),
                            event.getTimestamp()
                        ));
                    }
                }

                @Override
                public void processElement2(User user, Context ctx, Collector<EventUserEnrichment> out) throws Exception {
                    // In this method is trying to update the state of User
                    // So the event can fetch the User to join
                    userMap.put(user.getId(), user);
                    System.out.println("Stored user: " + user.getName());
                }

            });

       
        enrichedStream.print("Enriched Events");

        env.execute("Enrichment with CoProcessFunction");
    }
}
