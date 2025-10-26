package flink.self.traning.statefulTransformation;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.self.traning.customDataGen.EventGeneratorFunction;
import flink.self.traning.models.Event;

/*
 * This practice is for windowing without watermark 
 */

public class GroupByWindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource<Event> dgs = new DataGeneratorSource<>(new EventGeneratorFunction(), 
                                                                    Long.MAX_VALUE, 
                                                                    Types.POJO(Event.class));

        DataStream<Event> dse = env.fromSource(dgs,
                                                WatermarkStrategy.noWatermarks(),
                                               "Events generator");

        // Group by event name
        KeyedStream<Event, String> keyedEvent = dse.keyBy(event -> event.getName());

        // Aggregate on the event counts by event name
        // DataStream<String> aggEvent;
        // aggEvent = keyedEvent.window(GlobalWindows.create())
        //         .trigger(CountTrigger.of(1))
        //         .aggregate(
        //                 new AggregateFunction<Event, Integer, Integer>() {
                            
        //                     @Override
        //                     public Integer createAccumulator() {
        //                         return 0;
        //                     }
                            
        //                     @Override
        //                     public Integer add(Event value, Integer accumulator) {
        //                         return accumulator  + 1;
        //                     }
                            
        //                     @Override
        //                     public Integer getResult(Integer accumulator) {
        //                         return accumulator;
        //                     }
                            
        //                     @Override
        //                     public Integer merge(Integer a, Integer b) {
        //                         return a + b;
        //                     }
                            
        //                 },
        //                 new ProcessWindowFunction<Integer, String, String, GlobalWindow>() {
                            
        //                     @Override
        //                     public void process(String key,
        //                             ProcessWindowFunction<Integer, String, String, GlobalWindow>.Context context,
        //                             Iterable<Integer> elements, Collector<String> result) throws Exception {
        //                         for(Integer element : elements){
        //                             result.collect("The event " + key + " triggered in total " + element + " times");
        //                         }
        //                     }
                            
                            
                            
        //                 }
        //         );
        
        // Aggregate on the event counts by event name in tumbling window
        DataStream<String> aggEventTumble = keyedEvent.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                                       .aggregate(
                                            new AggregateFunction<Event, Integer,Integer>() {

                                                @Override
                                                public Integer createAccumulator() {
                                                    return 0;
                                                }

                                                @Override
                                                public Integer add(Event value, Integer accumulator) {
                                                    return accumulator + 1;
                                                }

                                                @Override
                                                public Integer getResult(Integer accumulator) {
                                                    return accumulator;
                                                }

                                                @Override
                                                public Integer merge(Integer a, Integer b) {
                                                    return a + b;
                                                }
                                            },
                                            new ProcessWindowFunction<Integer, String, String, TimeWindow>() {

                                                @Override
                                                public void process(String key,
                                                                    ProcessWindowFunction<Integer, String, String, TimeWindow>.Context context,
                                                                    Iterable<Integer> elements,
                                                                    Collector<String> result) throws Exception {
                                                    

                                                    TimeWindow window = context.window();
                                                    long windowStart = window.getStart();
                                                    long windowEnd = window.getEnd();
                                                    Instant windowStartInstant = Instant.ofEpochMilli(windowStart);
                                                    Instant windowEndInstant = Instant.ofEpochMilli(windowEnd);
                                                    LocalDateTime localDateTimeWindowStart = LocalDateTime.ofInstant(windowStartInstant, ZoneId.of("Europe/Paris"));
                                                    LocalDateTime localDateTimeWindowEnd = LocalDateTime.ofInstant(windowEndInstant, ZoneId.of("Europe/Paris"));

                                                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                                    String formattedDateTimeWindowStart = localDateTimeWindowStart.format(formatter);
                                                    String formattedDateTimeWindowEnd = localDateTimeWindowEnd.format(formatter);

                                                    for (Integer element : elements) {
                                                        result.collect("The event " + key + " triggered " + element + " times in window [" + formattedDateTimeWindowStart + " - " + formattedDateTimeWindowEnd + "]");
                                                    }

                                                }
                                                
                                            }
                                       );

        // aggEvent.print("print out aggregation without window");
        aggEventTumble.print("Print out aggregation with tumbling window");
        env.execute();
    }
}
