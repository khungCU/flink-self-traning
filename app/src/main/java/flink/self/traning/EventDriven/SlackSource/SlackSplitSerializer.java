package flink.self.traning.EventDriven.SlackSource;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class SlackSplitSerializer implements SimpleVersionedSerializer<SlackSplit>{

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(SlackSplit split) throws IOException {
        // SlackSplit has no mutable state (splitId is constant)
        // Return empty array since there's nothing to serialize
        return new byte[0];
    }

    @Override
    public SlackSplit deserialize(int version, byte[] serialized) throws IOException {
        // Since SlackSplit has no state, just create new instance
        return new SlackSplit();
    }

}
