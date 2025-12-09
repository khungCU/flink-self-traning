package flink.self.training.utils;

import org.apache.flink.api.common.functions.FilterFunction;

import com.flink.self.training.Client;


public class FilterVip implements FilterFunction<Client> {

    @Override
    public boolean filter(Client value) throws Exception {
        return value.getVip() == true;
    }
}
