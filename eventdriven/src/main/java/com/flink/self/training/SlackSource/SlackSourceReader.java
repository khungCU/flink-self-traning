package com.flink.self.training.SlackSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.bolt.socket_mode.SocketModeApp;
import com.slack.api.model.event.MessageEvent;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Source<T, SplitT, EnumChkT>
// T - The type of the record emitted by this source reader.
// SplitT - The type of the source splits.
public class SlackSourceReader implements  SourceReader<SlackMsgModel, SlackSplit>{

    private static final Logger LOG = LoggerFactory.getLogger(SlackSourceReader.class);
    private static final int BUFFER_CAPACITY = 1000;

    // Provides metadata and control for this reader instance (e.g., subtask index, parallelism)
    public final SourceReaderContext context;
    public final String botToken;
    public final String appToken;
    public final String sourceChannelId;

    // Bridge between Slack's push model and Flink's pull model with bounded capacity
    public final BlockingQueue<SlackMsgModel> messageBuffer = new ArrayBlockingQueue<>(BUFFER_CAPACITY);

    // Signal when new data is available to avoid busy-polling
    private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();
    private final AtomicBoolean isConnected = new AtomicBoolean(false);

    private SocketModeApp socketModeApp;
    private App app;
    private SlackSplit currentSplit;

    public SlackSourceReader(
        SourceReaderContext context,
        String botToken,
        String appToken,
        String sourceChannelId
    ){
        this.context = context;
        this.botToken = botToken;
        this.appToken = appToken;
        this.sourceChannelId = sourceChannelId;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing SlackSourceReader");
        isConnected.set(false);
        if (socketModeApp != null) {
            socketModeApp.stop();
            socketModeApp = null;
        }
        messageBuffer.clear();
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<SlackMsgModel> output) throws Exception {
        // Pull from buffer
        SlackMsgModel message = messageBuffer.poll();
        if (message != null) {
            output.collect(message);
            // Check if more data is immediately available
            return messageBuffer.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<SlackSplit> snapshotState(long checkpointId) {
        if (currentSplit != null) {
            return Collections.singletonList(currentSplit);
        }
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // If buffer has data or not connected yet, return immediately
        if (!messageBuffer.isEmpty() || !isConnected.get()) {
            return CompletableFuture.completedFuture(null);
        }

        // Otherwise, return future that completes when new data arrives
        // This avoids busy-polling when buffer is empty
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<SlackSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }

        // Prevent duplicate connections during recovery
        if (socketModeApp != null) {
            LOG.warn("Socket already exists, closing before creating new connection");
            try {
                socketModeApp.stop();
            } catch (Exception e) {
                LOG.error("Error stopping existing socket", e);
            }
        }

        // Because we only have one split in this Source
        currentSplit = splits.get(0);
        LOG.info("Adding split: {}", currentSplit.splitId());

        // Setup Slack connection
        app = new App(AppConfig.builder().singleTeamBotToken(botToken).build());

        // Register event handler - pushes to buffer
        app.event(MessageEvent.class, (payload, ctx) -> {
            try {
                MessageEvent event = payload.getEvent();
                String channelId = event.getChannel();
                String channelType = event.getChannelType();
                String text = event.getText();

                // Null-safety checks
                if (channelId != null && sourceChannelId.equals(channelId) && text != null) {
                    SlackMsgModel slackEvent = new SlackMsgModel(channelId, channelType, text);

                    // Try to add to buffer, drop if full (backpressure handling)
                    boolean added = messageBuffer.offer(slackEvent);
                    if (!added) {
                        LOG.warn("Message buffer full, dropping message from channel: {}", channelId);
                    } else {
                        // Signal that data is available
                        CompletableFuture<Void> currentFuture = availabilityFuture;
                        availabilityFuture = new CompletableFuture<>();
                        currentFuture.complete(null);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error processing Slack message", e);
            }

            return ctx.ack();
        });

        // Start socket connection asynchronously (non-blocking for event-driven pipeline)
        try {
            socketModeApp = new SocketModeApp(appToken, app);
            socketModeApp.startAsync();

            // Mark as connected after async start initiated
            isConnected.set(true);
            LOG.info("Slack socket connection started for channel: {}", sourceChannelId);
        } catch (Exception e) {
            LOG.error("Failed to start Slack socket", e);
            throw new RuntimeException("Failed to start Slack socket", e);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        // Streaming source - this is normal, just continue
    }
}
