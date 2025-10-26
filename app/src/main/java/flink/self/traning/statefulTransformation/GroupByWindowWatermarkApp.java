package flink.self.traning.statefulTransformation;

import java.time.Duration;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.self.traning.models.Event;

public class GroupByWindowWatermarkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        /*
         * Event time generates watermarks, which trigger window computations.
         * Event time is used to calculate watermarks, and watermarks determine when
         * windows should fire.
         * Event timestamps drive watermark progression, which in turn triggers window
         * evaluation.
         */

        // Define a watermark strategy in the source
        DataStream<Event> dsSource = env.fromElements(
            new Event("login", 1, 1000L),
            new Event("login", 2, 3000L),
            new Event("search", 1, 5000L),
            new Event("search", 1, 5000L),
            new Event("search", 1, 6000L),
            new Event("search", 2, 6000L),
            new Event("search", 1, 10000L),
            new Event("button click", 1, 3000L),
            new Event("button click", 1, 8000L),
            new Event("logout", 1,18000L),
            new Event("search", 2, 12000L),
            new Event("logout", 2, 15000L),
            new Event("logout", 1, 50000L)
                ).assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );


        KeyedStream<Event, Integer> ks = dsSource
                                         .keyBy(element -> element.getUserId());

        DataStream<String> res = ks.window(TumblingEventTimeWindows.of(Time.seconds(3)))
                                   .aggregate(
                                        new AggregateFunction<Event, Integer, Integer>(){

                                            @Override
                                            public Integer createAccumulator() {
                                                // Init aggregator, count start from 0
                                                return 0;
                                            }

                                            @Override
                                            public Integer add(Event value, Integer accumulator) {
                                                // Count event is nothing but plus one each time
                                                return accumulator + 1;
                                            }

                                            @Override
                                            public Integer getResult(Integer accumulator) {
                                                // return the count result
                                                return accumulator;                                         
                                            }

                                            @Override
                                            public Integer merge(Integer a, Integer b) {
                                                return a + b;
                                            }
                                        },
                                        new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {

                                            @Override
                                            public void process(Integer key,
                                                                ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context,
                                                                Iterable<Integer> elements, 
                                                                Collector<String> result) throws Exception {
                                            
                                                TimeWindow window = context.window();
                                                long windowStart = window.getStart();
                                                long windowEnd = window.getEnd();
                                           
                                                for (Integer element : elements) {
                                                    result.collect("The userid:  " + key + " current event count " + element + " times in window [" + windowStart + " - " + windowEnd + "]");
                                                }
                                                
                                            }
                                            
                                        }
                                   );
        res.print("DataStream with watermark");
        env.execute();
        

    }
    
}
