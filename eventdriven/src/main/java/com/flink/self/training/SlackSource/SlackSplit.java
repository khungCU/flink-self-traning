package com.flink.self.training.SlackSource;

import org.apache.flink.api.connector.source.SourceSplit;

public class SlackSplit implements SourceSplit {
    public String splitId = "slack-socket";

    @Override
    public String splitId() {
        return this.splitId;
    }

}
