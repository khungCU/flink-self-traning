package workflows;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import model.CdcEvent;
import model.MessageNormalized;
import reconciliation.SchemaNormalizer;
import reconciliation.TableRegistry;

/**
 * Workflow for CDC sync: MySQL → Postgres.
 *
 * Supports two equivalent configuration styles — use whichever fits the context:
 *
 * Style 1 — Builder (validated at build() time):
 * <pre>
 *   new DBSyncWithSchemaWorkflow.Builder()
 *       .cdcSourceDataStream(cdcStream)
 *       .registry(new TableRegistry())
 *       .sink(new PGSinker(...))
 *       .build()
 *       .execute();
 * </pre>
 *
 * Style 2 — Direct construction with chained setters (validated at execute() time):
 * <pre>
 *   new DBSyncWithSchemaWorkflow()
 *       .setSource(cdcStream)
 *       .setRegistry(new TableRegistry())
 *       .setSink(new PGSinker(...))
 *       .execute();
 * </pre>
 */
public class DBSyncWithSchemaWorkflow {

    private DataStream<CdcEvent> cdcSourceDataStream;
    private TableRegistry registry;
    private Sink<MessageNormalized> sink;
    private String jobName = "MySQL to Postgres CDC Mirror";

    /** Direct construction — configure via setSource / setRegistry / setSink before calling execute(). */
    public DBSyncWithSchemaWorkflow() {}

    private DBSyncWithSchemaWorkflow(Builder builder) {
        this.cdcSourceDataStream = builder.cdcSourceDataStream;
        this.registry = builder.registry;
        this.sink = builder.sink;
        this.jobName = builder.jobName;
    }

    // --- Fluent setters ---

    public DBSyncWithSchemaWorkflow setSource(DataStream<CdcEvent> cdcSourceDataStream) {
        this.cdcSourceDataStream = cdcSourceDataStream;
        return this;
    }

    public DBSyncWithSchemaWorkflow setRegistry(TableRegistry registry) {
        this.registry = registry;
        return this;
    }

    public DBSyncWithSchemaWorkflow setSink(Sink<MessageNormalized> sink) {
        this.sink = sink;
        return this;
    }

    public DBSyncWithSchemaWorkflow setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    // --- Execution ---

    /**
     * Validates all required fields, assembles and executes the CDC pipeline
     * on the environment that cdcSourceDataStream is already bound to.
     * Checkpointing must be configured on the environment before calling this.
     */
    public void execute() throws Exception {
        Preconditions.checkNotNull(cdcSourceDataStream, "cdcSourceDataStream must not be null — call setSource() or use the Builder");
        Preconditions.checkNotNull(registry, "registry must not be null — call setRegistry() or use the Builder");
        Preconditions.checkNotNull(sink, "sink must not be null — call setSink() or use the Builder");

        StreamExecutionEnvironment env = cdcSourceDataStream.getExecutionEnvironment();

        // Normalize CDC events into typed POJOs, routed via per-table side outputs
        SingleOutputStreamOperator<MessageNormalized> processed = cdcSourceDataStream
                .keyBy(CdcEvent::getTable)
                .process(new SchemaNormalizer(registry));

        // Log schema-drift events (unknown tables) for alerting
        processed.getSideOutput(registry.getUnknownTag()).print("schema-drift");

        // Union all known-table side outputs into a single stream
        DataStream<MessageNormalized> normalizedStream = null;
        for (OutputTag<MessageNormalized> tag : registry.getAllKnownTags()) {
            DataStream<MessageNormalized> sideOutput = processed.getSideOutput(tag);
            normalizedStream = (normalizedStream == null) ? sideOutput : normalizedStream.union(sideOutput);
        }

        if (normalizedStream == null) {
            throw new IllegalStateException("TableRegistry has no registered tables — nothing to sink");
        }
        normalizedStream.sinkTo(sink);

        env.execute(jobName);
    }

    // --- Builder ---

    public static class Builder {

        private DataStream<CdcEvent> cdcSourceDataStream;
        private TableRegistry registry;
        private Sink<MessageNormalized> sink;
        private String jobName = "MySQL to Postgres CDC Mirror";

        /** The CDC input stream, already bound to a configured StreamExecutionEnvironment. */
        public Builder cdcSourceDataStream(DataStream<CdcEvent> cdcSourceDataStream) {
            this.cdcSourceDataStream = cdcSourceDataStream;
            return this;
        }

        /** Schema registry — defines which tables are known and what their PKs are. */
        public Builder registry(TableRegistry registry) {
            this.registry = registry;
            return this;
        }

        /** Destination sink — PGSinker in production, a test sink in tests. */
        public Builder sink(Sink<MessageNormalized> sink) {
            this.sink = sink;
            return this;
        }

        /** Overrides the default Flink job name shown in the JobManager UI. */
        public Builder jobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        /** Validates all required fields and returns a configured workflow instance. */
        public DBSyncWithSchemaWorkflow build() {
            Preconditions.checkNotNull(cdcSourceDataStream, "cdcSourceDataStream must not be null");
            Preconditions.checkNotNull(registry, "registry must not be null");
            Preconditions.checkNotNull(sink, "sink must not be null");
            return new DBSyncWithSchemaWorkflow(this);
        }
    }
}
