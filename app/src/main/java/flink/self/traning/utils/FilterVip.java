package flink.self.traning.utils;

import org.apache.flink.api.common.functions.FilterFunction;

import flink.self.traning.models.Client;


public class FilterVip implements FilterFunction<Client> {

    @Override
    public boolean filter(Client value) throws Exception {
        return value.getVip() == true;
    }
}
