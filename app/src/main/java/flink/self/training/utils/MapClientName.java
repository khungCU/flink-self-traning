package flink.self.training.utils;

import org.apache.flink.api.common.functions.MapFunction;

import com.flink.self.training.Client;

public class MapClientName implements MapFunction<Client, Client> {

    @Override
    public Client map(Client value) throws Exception {
        String newName = value.getName().toUpperCase();
        value.setName(newName);
        return value;
    }

}
