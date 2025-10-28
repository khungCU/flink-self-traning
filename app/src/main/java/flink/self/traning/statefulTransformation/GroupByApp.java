package flink.self.traning.statefulTransformation;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import flink.self.traning.models.Client;

public class GroupByApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Client> ds = env.fromElements(
                new Client(1, "Ken", "male", true, 100),
                new Client(2, "Joe", "male", true, 60),
                new Client(3, "Kate", "female", false, 80),
                new Client(4, "Alex", "female", true, 40),
                new Client(5, "Dow", "female", false, 50)
        );

        // Calculate the total score using keyBy + reduce method
        // be aware of the hot partition issues
        // KeyedStream<T, KEY>
        //      T - The type of the elements in the Keyed Stream.
        //      KEY - The type of the key in the Keyed Stream.
        KeyedStream<Client, String> ks = ds.keyBy(k -> "all");
    

        // Use reduce to aggregate the total score
        // ReduceFunction works with the same input/output type (Client -> Client)
        // with reduce method it can also provide a ProcessWindowFunction so it can change result type
        // also the only way to get the context is also to provide a ProcessWindowFunction
        SingleOutputStreamOperator<Client> aggregated = ks.reduce(
            new ReduceFunction<Client>() {
                @Override
                public Client reduce(Client client1, Client client2) throws Exception {
                    // Combine two clients by summing their scores
                    // Keep first client's info, accumulate the score
                    return new Client(
                        client1.getId(),
                        client1.getName(),
                        client1.getGender(),
                        client1.getVip(),
                        client1.getScore() + client2.getScore()
                    );
                }
            }
        );

        // Map the Client result to a String for printing
        SingleOutputStreamOperator<String> res = aggregated.map(
            client -> "The total score of all clients is: " + client.getScore()
        );

        // Use aggregate() with WINDOW to aggregate the total score
        // AggregateFunction<IN, ACC, OUT> requires a window on KeyedStream
        //   IN  - Input type (Client)
        //   ACC - Accumulator type (Integer - running sum)
        //   OUT - Output type (String - formatted result)
        // State are store in the window state 
        // Still risk for the Hot partition problem - all data flows through one operator instance
        /*
         * ex:
         * Key: "all"
         * Window State:
         * - Accumulator: 100
         * - Trigger State: count = 1
         */ 
        SingleOutputStreamOperator<String> aggregated2 = ks
            .window(GlobalWindows.create())  // Create a global window
            .trigger(CountTrigger.of(5))     // Trigger after 5 elements (all clients)
            .aggregate(
                new AggregateFunction<Client, Integer, String>() {

                    @Override
                    public Integer createAccumulator() {
                        // Initialize the accumulator with 0
                        return 0;
                    }

                    @Override
                    public Integer add(Client value, Integer accumulator) {
                        // Add the current client's score to the accumulator
                        return accumulator + value.getScore();
                    }

                    @Override
                    public String getResult(Integer accumulator) {
                        // Convert the final accumulator to output format
                        return "The total score (aggregate) is: " + accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        // Merge two accumulators (used in parallel processing)
                        return a + b;
                    }

                }
            );

        // 
        res.print("reduceFlow");
        aggregated2.print("aggregateFlow");

        env.execute();
    }

}
