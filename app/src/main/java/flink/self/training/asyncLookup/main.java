package flink.self.training.asyncLookup;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Location> ds = env.fromElements(
            new Location(48.8575d ,2.3514d, "Paris"),
            new Location(25.0330d, 121.5654d, "Taipei"),
            new Location(35.6764d, 139.6500d, "Tokyo"),
            new Location(40.7128d, -74.0060d, "New York")
        );

        String apiKey = "77ffaf30375fb8381bd690201a06cd5e";
        DataStream<EnrichLocation> enriched = AsyncDataStream.unorderedWait(ds,
                                                      new AsyncWeather(apiKey),
                                                      5000,
                                                      TimeUnit.MILLISECONDS);

        enriched.print();
        
        env.execute();
    }

}
