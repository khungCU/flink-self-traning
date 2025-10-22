package flink.self.traning.customDataGen;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.self.traning.models.Event;


public class EventGenerator {
    public static void main(String[] args) throws Exception{
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        

        DataGeneratorSource<Event> source =
                new DataGeneratorSource<Event>(new EventGeneratorFunction(), 
                                                Long.MAX_VALUE,
                                                Types.POJO(Event.class));

        DataStreamSource<Event> stream =
                env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "Generator Source of Events");
        
        stream.print();

        env.execute();
        
    }

}
