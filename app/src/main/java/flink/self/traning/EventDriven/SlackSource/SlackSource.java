package flink.self.traning.EventDriven.SlackSource;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;


// Type Parameters
// T - The type of records produced by the source.
// SplitT - The type of splits handled by the source.
// EnumChkT - The type of the enumerator checkpoints.
public class SlackSource implements Source<SlackMsgModel, SlackSplit, SlackEnumeratorState> {

    public String botToken;
    public String appToken;
    public String sourceChannelId;

    // Reuse serializer instances for better performance
    private static final SlackSplitSerializer SPLIT_SERIALIZER = new SlackSplitSerializer();
    private static final SlackEnumeratorStateSerializer STATE_SERIALIZER = new SlackEnumeratorStateSerializer();

    public SlackSource(String botToken,
                String appToken,
                String sourceChannelId) {
        this.botToken = botToken;
        this.appToken = appToken;
        this.sourceChannelId = sourceChannelId;
    }

    @Override
    public Boundedness getBoundedness() {
        // Streaming = CONTINUOUS_UNBOUNDED
        // Batch = BOUNDED
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<SlackSplit, SlackEnumeratorState> createEnumerator(SplitEnumeratorContext<SlackSplit> enumContext) throws Exception {
        // Fresh start - create new enumerator
        return new SlackSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<SlackSplit, SlackEnumeratorState> restoreEnumerator(SplitEnumeratorContext<SlackSplit> enumContext, 
                                                                               SlackEnumeratorState checkpoint) throws Exception {
        // Recovery - restore from checkpoint
        return new SlackSplitEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<SlackSplit> getSplitSerializer() {
        return SPLIT_SERIALIZER;
    }

    @Override
    public SimpleVersionedSerializer<SlackEnumeratorState> getEnumeratorCheckpointSerializer() {
        return STATE_SERIALIZER;
    }

    @Override
    public SourceReader<SlackMsgModel, SlackSplit> createReader(SourceReaderContext readerContext) throws Exception {
        // Create reader with connection details
        return new SlackSourceReader(readerContext, botToken, appToken, sourceChannelId);
    }


}
