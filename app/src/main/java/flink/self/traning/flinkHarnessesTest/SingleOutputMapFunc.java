package flink.self.traning.flinkHarnessesTest;

import org.apache.flink.api.common.functions.MapFunction;

public class SingleOutputMapFunc implements MapFunction<String, String> {
    @Override
    public String map(String value) throws Exception {
        return value.toUpperCase();
    }

}
