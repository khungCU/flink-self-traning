package flink.self.traning;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import flink.self.traning.models.Client;

public class SplitApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Client> ds = env.fromElements(
                new Client(1, "Ken", "male", true),
                new Client(2, "Joe", "male", true),
                new Client(3, "Alex", "female", true),
                new Client(4, "Kate", "female", false)
        );

        // Introduce the process function 
        // Use ProcessFunction when need input -> context + state + timers and outputs
        // With Anonymous class (ProcessFunction is not a functional interface)
        // DataStream<String> res = ds.process(
        //         new ProcessFunction<Client, String>() {
        //             @Override
        //             public void processElement(Client client, Context ctx, Collector<String> out) throws Exception {
        //                 out.collect(client.getName() + ", " + ctx.timestamp());
        //             }
        //         }
        // ).returns(String.class);

        // Split the stream with process function and side output
        // Step 1: Define the OutputTag for non-VIP clients
        OutputTag<String> nonVipTag = new OutputTag<String>("non-vip-output"){};

        // Step 2: Process and split the stream
        // SingleOutputStreamOperator is a subclass of DataStream that represents a
        // stream created by applying a transformation operation. It has all the
        // features of DataStream plus additional capabilities EX: side output.
        SingleOutputStreamOperator<String> vipStream = ds.process(
            new ProcessFunction<Client, String>() {
                @Override
                public void processElement(Client client, Context ctx, Collector<String> out) throws Exception{
                    if (client.getVip()) {
                        // Main output: VIP clients
                        out.collect(client.getName() + " is vip");
                    } else {
                        // Side output: Non-VIP clients
                        ctx.output(nonVipTag, client.getName() + " is not vip");
                    }
                }
            }
        );

        // Step 3: Get the side output stream
        DataStream<String> nonVipStream = vipStream.getSideOutput(nonVipTag);

        // Step 4: Print both streams
        vipStream.print("Vip Stream");
        nonVipStream.print("Non Vip Stream");

        env.execute();
    }

    
    
    


}
