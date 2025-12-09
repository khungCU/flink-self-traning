package com.flink.self.training.SlackSource;

public class SlackEnumeratorState {

    // Tracks whether the split has been given to a reader. 
    public boolean assigned;

    // Stores the actual split object that hasn't been assigned yet.
    public SlackSplit pendingSplit;

    public SlackEnumeratorState(){
    }

    public SlackEnumeratorState(boolean assigned, SlackSplit pendingSplit) {
        this.assigned = assigned;
        this.pendingSplit = pendingSplit;
    }

    public boolean isAssigned() {
        return assigned;
    }

    public SlackSplit getPendingSplit() {
        return pendingSplit;
    }
}