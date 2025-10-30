package flink.self.traning.statefulTransformation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

public class EnrichmentWindowFlatJoinApp {
    // Manual windowing with connect() using CoProcessFunction
    // This example shows how to:
    // 1. Use event time and watermarks
    // 2. Buffer events in windows
    // 3. Use timers to trigger window processing

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // Single parallelism for easier debugging

        // Create event stream with event time
        DataStream<Event> eventSource = env.fromElements(
            new Event("login", 1, 1000L),
            new Event("search", 1, 2000L),
            new Event("search", 1, 3000L),
            new Event("purchase", 1, 4000L),
            new Event("login", 2, 5000L),
            new Event("search", 2, 6000L),
            new Event("search", 1, 11000L),  // Next window
            new Event("purchase", 1, 12000L),
            new Event("search", 2, 15000L)
        )
        // IMPORTANT: Assign event time and watermarks
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))  // Allow 1 second out-of-order
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())  // Use event's timestamp field
        );

        // Create user stream with event time
        DataStream<User> userSource = env.fromElements(
            new User(1, "Ken", "male", true),
            new User(2, "Alex", "female", false)
        )
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((user, timestamp) -> 0L)  // Users don't have timestamp, use 0
        );

        // Connect streams and implement manual windowing
        DataStream<Tuple4<String, String, Long, String>> res = eventSource.connect(userSource)
            .keyBy(event -> event.getUserId(), user -> user.getId())
            .process(new ManualWindowCoProcessFunction(10000L));  // 10-second windows

        res.print();
        env.execute("Manual Windowing with Connect");
    }

    /**
     * Manual windowing implementation using CoProcessFunction
     * Window size: configurable (e.g., 10 seconds)
     * Window type: Tumbling windows
     */
    static class ManualWindowCoProcessFunction
            extends KeyedCoProcessFunction<Integer, Event, User, Tuple4<String, String, Long, String>> {

        private final long windowSize;  // Window size in milliseconds

        // State: store user information
        private ValueState<User> userState;

        // State: buffer events per window
        // Key = window end time, Value = list of events in that window
        private ListState<EventWithWindow> eventBuffer;

        public ManualWindowCoProcessFunction(long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize user state
            userState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("user-state", User.class)
            );

            // Initialize event buffer
            eventBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("event-buffer", EventWithWindow.class)
            );
        }

        @Override
        public void processElement1(Event event, Context ctx,
                Collector<Tuple4<String, String, Long, String>> out) throws Exception {

            // ===== KEY CONCEPT 1: Get Event Time =====
            // ctx.timestamp() returns the event time of current element
            // This comes from the watermark strategy we defined
            long eventTime = ctx.timestamp();

            System.out.println(String.format(
                "[processElement1] Event: %s, userId=%d, eventTime=%d, currentWatermark=%d",
                event.getName(), event.getUserId(), eventTime, ctx.timerService().currentWatermark()
            ));

            // ===== KEY CONCEPT 2: Calculate Window =====
            // Determine which window this event belongs to
            long windowEnd = getWindowEnd(eventTime);
            long windowStart = windowEnd - windowSize;

            System.out.println(String.format(
                "  → Event belongs to window [%d, %d)", windowStart, windowEnd
            ));

            // ===== KEY CONCEPT 3: Buffer Event =====
            // Store event with its window information
            eventBuffer.add(new EventWithWindow(event, windowEnd));

            // ===== KEY CONCEPT 4: Register Timer =====
            // Schedule timer to fire when window closes
            // Timer fires when watermark >= windowEnd
            ctx.timerService().registerEventTimeTimer(windowEnd);

            System.out.println(String.format(
                "  → Timer registered for windowEnd=%d (will fire when watermark >= %d)",
                windowEnd, windowEnd
            ));
        }

        @Override
        public void processElement2(User user, Context ctx,
                Collector<Tuple4<String, String, Long, String>> out) throws Exception {

            System.out.println(String.format(
                "[processElement2] User: %s (id=%d), currentWatermark=%d",
                user.getName(), user.getId(), ctx.timerService().currentWatermark()
            ));

            // Update user state (for enrichment)
            userState.update(user);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                Collector<Tuple4<String, String, Long, String>> out) throws Exception {

            // ===== KEY CONCEPT 5: Timer Fires When Window Closes =====
            // This is called when watermark >= timestamp (window end time)
            long windowEnd = timestamp;
            long windowStart = windowEnd - windowSize;

            System.out.println(String.format(
                "\n[onTimer] Window [%d, %d) is closing! currentWatermark=%d",
                windowStart, windowEnd, ctx.timerService().currentWatermark()
            ));

            // Get user for enrichment
            User user = userState.value();

            if (user == null) {
                System.out.println("  → No user data available, skipping enrichment");
                return;
            }

            // ===== KEY CONCEPT 6: Process All Events in This Window =====
            List<Event> windowEvents = new ArrayList<>();
            List<EventWithWindow> remainingEvents = new ArrayList<>();

            // Separate events: current window vs future windows
            for (EventWithWindow eww : eventBuffer.get()) {
                if (eww.windowEnd == windowEnd) {
                    windowEvents.add(eww.event);  // This window
                } else {
                    remainingEvents.add(eww);     // Future windows
                }
            }

            System.out.println(String.format(
                "  → Processing %d events in this window", windowEvents.size()
            ));

            // Emit enriched and flattened results
            for (Event event : windowEvents) {
                out.collect(new Tuple4<>(
                    user.getName(),
                    event.getName(),
                    event.getTimestamp(),
                    "Flat 1"
                ));
                out.collect(new Tuple4<>(
                    user.getName(),
                    event.getName(),
                    event.getTimestamp(),
                    "Flat 2"
                ));

                System.out.println(String.format(
                    "  → Emitted: user=%s, event=%s, timestamp=%d",
                    user.getName(), event.getName(), event.getTimestamp()
                ));
            }

            // Clean up: keep only future window events
            eventBuffer.update(remainingEvents);

            System.out.println(String.format(
                "  → Window closed. Remaining buffered events: %d\n",
                remainingEvents.size()
            ));
        }

        /**
         * Calculate window end time for a given event time
         * For tumbling windows: windowEnd = ceil(eventTime / windowSize) * windowSize
         */
        private long getWindowEnd(long eventTime) {
            return eventTime - (eventTime % windowSize) + windowSize;
        }
    }

    /**
     * Helper class to store event with its window assignment
     */
    static class EventWithWindow {
        public Event event;
        public long windowEnd;

        public EventWithWindow() {}  // Required for Flink serialization

        public EventWithWindow(Event event, long windowEnd) {
            this.event = event;
            this.windowEnd = windowEnd;
        }
    }
}
