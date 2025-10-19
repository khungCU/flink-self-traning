package flink.self.traning.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import flink.self.traning.models.Client;

/* the type in FlatMapFunction are  
    FlatMapFunction<IN, OUT> means:
        First type (IN): The input type - what comes INTO the function
        Second type (OUT): The output type - what the Collector will collect
*/

 /*
  The Collector collects the object(s) and feeds them back into the stream.
    map() = 1 input → 1 output (no collector needed, just return)
    flatMap() = 1 input → 0 or many outputs (collector needed to handle multiple outputs)
*/

public class FlattenClient implements FlatMapFunction<Client, String> {
    @Override
    public void flatMap(Client value, Collector<String> out) throws Exception {
        out.collect("Client " + value.getName() + " " + (value.getVip() ? "is a vip" : "is not a vip"));
        out.collect(value.getName() + " is a " + value.getGender());
    }
}
