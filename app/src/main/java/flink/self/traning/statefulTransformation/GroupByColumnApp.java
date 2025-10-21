package flink.self.traning.statefulTransformation;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import flink.self.traning.models.Client;

public class GroupByColumnApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Client> ds = env.fromElements(
                new Client(1, "Ken", "male", true, 100),
                new Client(2, "Joe", "male", true, 60),
                new Client(3, "Kate", "female", false, 80),
                new Client(4, "Alex", "female", true, 40),
                new Client(5, "Dow", "female", false, 50)
        );

        /* Here is to calculate average score by genders */
        
        DataStream<Tuple3<String, Integer, Integer>> dst = ds.map(
            new MapFunction<Client, Tuple3<String, Integer, Integer>>() {
                @Override
                public Tuple3<String, Integer, Integer> map(Client client) throws Exception {
                    return new Tuple3<>(client.getGender(), client.getScore(), 1);
                }
            }
        );
        // key by gender
        KeyedStream<Tuple3<String, Integer, Integer>, String> kds = dst.keyBy(k -> k.f0);
        // reduce score calculate the sum and count
        DataStream<Tuple3<String, Integer, Integer>> transform = kds.reduce(
            new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                @Override
                public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) throws Exception {
                    return new Tuple3<String, Integer, Integer>(
                        value1.f0, // gender
                        value1.f1 + value2.f1, // Sum of score
                        value1.f2 + value2.f2 // Sum of count
                    );
                }
            }
        );
        DataStream<String> res = transform.map(
            new MapFunction<Tuple3<String,Integer,Integer>,String>() {
                @Override
                public String map(Tuple3<String, Integer, Integer> value) throws Exception {
                    return "The current running average of " + value.f0 + " is " + (double)(value.f1 / value.f2);
                }   
            }
        );
        res.print("The average using reduce");

        /* What a work just to calculate the average */
        /* Below is to calculate the average using aggregate */
        KeyedStream<Client, String> ks = ds.keyBy(k -> k.getGender());
        SingleOutputStreamOperator<String> resagg = ks.window(GlobalWindows.create())
          .trigger(CountTrigger.of(1))
          .aggregate(
            new AggregateFunction<Client,Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

                @Override
                public Tuple2< Integer, Double> createAccumulator() {
                    // init Accumulator (count, sum)
                    return Tuple2.of(0,0d);
                }

                @Override
                public Tuple2<Integer, Double> add(Client value, Tuple2<Integer, Double> accumulator) {
                    // For each element that arrives in that window
                    // calculate count and running total
                    return new Tuple2<Integer, Double>(++accumulator.f0,  value.getScore() + accumulator.f1);
                }

                @Override
                public Tuple2<Integer, Double> getResult(Tuple2<Integer, Double> accumulator) {
                    return accumulator;
                }


                @Override
                public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> a, Tuple2<Integer, Double> b) {
                    // When combining partial results (parallel processing)
                    return new Tuple2<Integer, Double>(a.f0 + b.f0, a.f1 + b.f1);
                }
            },
            new ProcessWindowFunction<Tuple2<Integer, Double>, String, String, GlobalWindow>() {
                @Override
                public void process(String key, Context context, Iterable<Tuple2<Integer, Double>> elements, Collector<String> out) throws Exception {
                    /*
                    * * @param key The key for which this window is evaluated.
                    *
                    * @param context The context in which the window is being evaluated.
                    *
                    * @param elements The elements in the window being evaluated.
                    *
                    * @param out A collector for emitting elements.
                    */
                    for(Tuple2<Integer, Double> element : elements){
                        double average = element.f1 / element.f0;
                        out.collect("Current " + key + " average is " + average);
                    }
                }

            }
        );

        resagg.print("The average using aggregate()");

        env.execute();
    }

}
