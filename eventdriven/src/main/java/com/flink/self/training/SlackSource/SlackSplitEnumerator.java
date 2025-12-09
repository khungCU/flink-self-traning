package com.flink.self.training.SlackSource;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

// SplitEnumerator<SplitT,CheckpointT>
/*
Responsibilities:
Creates splits - creates the Slack socket split
Waits for reader requests - Yes! Readers call context.sendSplitRequest()
Assigns splits - sends split to reader
Handles failures - gets splits back from failed readers
Manages checkpoints - saves state
*/
public class SlackSplitEnumerator implements SplitEnumerator<SlackSplit, SlackEnumeratorState>{

    // The context is the bridge between your enumerator and Flink's runtime.
    // Marked transient because it's provided by Flink and shouldn't be serialized
    private final transient SplitEnumeratorContext<SlackSplit> context;
    
    public SlackSplit pendingSplit;
    public boolean assigned = false;

    public SlackSplitEnumerator(SplitEnumeratorContext<SlackSplit> context) {
        this.context = context;
    }

    // Constructor for state recovery from checkpoint
    public SlackSplitEnumerator(SplitEnumeratorContext<SlackSplit> context, SlackEnumeratorState state) {
        this.context = context;
        if (state != null) {
            this.assigned = state.isAssigned();
            this.pendingSplit = state.getPendingSplit();
        }
    }

    // Create split (only if not already restored from checkpoint)
    @Override
    public void start() {
        if (pendingSplit == null && !assigned) {
            // create only one split
            pendingSplit = new SlackSplit();
        }
    }

    // Reader calls context.sendSplitRequest() → "Hey enumerator, give me work" and enumerator Assign split
    // subtaskId : ID of the SourceReader asking for work
    // hostname: Machine where that reader runs
    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // Don't give away split twice, Make sure we have a split to give
        if (!assigned && pendingSplit != null) {
            context.assignSplit(pendingSplit, subtaskId);
            assigned = true; // Remember we gave it away
            pendingSplit = null;  // Clear it - nothing left to give
        }
    }

    // Reader crashes or fails, Flink calls this method → "Reader 0 died, here are its splits back"
    @Override
    public void addSplitsBack(List<SlackSplit> splits, int subtaskId) {
        // One split comes back, you store it and mark `assigned = false` so it can be reassigned.
        if (!splits.isEmpty()) {
            pendingSplit = splits.get(0);
            assigned = false;
        }
    }

    // Reader will call handleSplitRequest() when ready
    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public SlackEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new SlackEnumeratorState(assigned, pendingSplit);
    }

    @Override
    public void close() throws IOException {
    }
    
}
