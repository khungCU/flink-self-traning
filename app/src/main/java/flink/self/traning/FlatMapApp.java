package flink.self.traning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import flink.self.traning.models.Client;
import flink.self.traning.utils.FlattenClient;

/*
 *  The CAPTCHA using flatmap is since the output stream and input stream are different types
 *  it takes String[] but outputs individual String objects via the Collector. 
 * Due to Java's type erasure, Flink cannot determine what type you're collecting at runtime, 
 * so you must explicitly tell it with .returns()
 */
public class FlatMapApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Client> ds = env.fromElements(
                new Client(1, "Ken", "male", true),
                new Client(2, "Joe", "male", true),
                new Client(3, "Alex", "female", true),
                new Client(4, "Kate", "female", false));
        
        /* Simple example for nested list */
        // String[] list1 = { "a", "b", "c" };
        // String[] list2 = { "d", "e", "f" };

        // DataStream<String[]> sds = env.fromElements(list1, list2);
        // DataStream<String> flattenedStrings = sds.flatMap(
        //         (String[] in, Collector<String> out) -> {
        //             for (String i : in) {
        //                 out.collect(i);
        //             }
        //         }).returns(String.class);

        // flatMap with lambda - when type needs to be specified
        // due to type erasure we have to specify the type when using lambda
        /*
         * Java's type erasure problem: When Java compiles generic code like
         * Collector<String>, it erases the type information at runtime. The compiled
         * code only knows there's a Collector, not that it collects String objects.
         * Flink needs to know the exact types to:
         * Serialize data across the network
         * Store data efficiently
         * Convert between formats
         */
        // DataStream<String> res = ds.flatMap(
        //         (Client in, Collector<String> out) -> {
        //             out.collect("Client " + in.getName() + " " + (in.getVip() ? "is a vip" : "is not a vip"));
        //             out.collect(in.getName() + " is a " + in.getGender());
        //         }).returns(String.class);

        // flatMap with anonymous class
        // no need the call return any more to handle the different type
        // DataStream<String> res = ds.flatMap(
        //     new FlatMapFunction<Client,String>() {
        //         @Override
        //         public void flatMap(Client in,  Collector<String> out) throws Exception {
        //             out.collect("Client " + in.getName() + " " + (in.getVip() ? "is a vip" : "is not a vip"));
        //             out.collect(in.getName() + " is a " + in.getGender());
        //         };
        //     }
        // );

        // flatMap with custom class
        DataStream<String> res = ds.flatMap(new FlattenClient());
            
        res.print();
        env.execute();

    }

}
