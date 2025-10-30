package flink.self.traning.statefulTransformation;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import flink.self.traning.models.Event;
import flink.self.traning.models.User;

public class EnrichmentFlatJoinApp {
    // here to use flat process join with ValueState
    public static void main(String[] args) throws Exception{
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

        
        DataStream<Tuple4<String, String, Long, String>> res = eventSource.connect(userSource)
                   .keyBy(event -> event.getUserId(),
                          user -> user.getId())
                   .process(
                       new KeyedCoProcessFunction<Integer, Event, User, Tuple4<String, String, Long, String>>() {

                            private ValueState<User> userValueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // Initialize user state
                                ValueStateDescriptor<User> userDescriptor =
                                    new ValueStateDescriptor<>(
                                        "userState",    // state name
                                        User.class      // value type (User object)
                                    );
                                userValueState = getRuntimeContext().getState(userDescriptor);
                            }

                            @Override
                            public void processElement1(Event event, Context ctx,
                                    Collector<Tuple4<String, String, Long, String>> out) throws Exception {
                                // here is to flattern the join result from one to two
                                // return user_name, event_name, timestamp, flat_nbr
                                User user = userValueState.value();
                                if (user != null) {
                                    out.collect(new Tuple4<>(user.getName(), event.getName(), event.getTimestamp(), "Flat 1"));
                                    out.collect(new Tuple4<>(user.getName(), event.getName(), event.getTimestamp(), "Flat 2"));
                                } else {
                                    out.collect(new Tuple4<>("No user match", event.getName(), event.getTimestamp(), "Flat X"));
                                }

                            }

                            @Override
                            public void processElement2(User user, Context ctx,
                                    Collector<Tuple4<String, String, Long, String>> out) throws Exception {
                                userValueState.update(user);
                            }

                       }
                   );

        res.print("Flat map Enrichment");
        env.execute();

    }

}
