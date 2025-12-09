package com.flink.self.training.SlackSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class SlackEnumeratorStateSerializer implements SimpleVersionedSerializer<SlackEnumeratorState>{

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(SlackEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            // Serialize the assigned flag
            out.writeBoolean(state.isAssigned());

            // Serialize whether we have a pending split
            boolean hasPendingSplit = state.getPendingSplit() != null;
            out.writeBoolean(hasPendingSplit);

            // If there's a pending split, we could serialize its ID
            // But since SlackSplit has a constant ID, we just need the flag

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public SlackEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {

            boolean assigned = in.readBoolean();
            boolean hasPendingSplit = in.readBoolean();

            SlackSplit pendingSplit = hasPendingSplit ? new SlackSplit() : null;

            return new SlackEnumeratorState(assigned, pendingSplit);
        }
    }

}
