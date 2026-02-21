package reconciliation;

import java.util.Map;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import model.CdcEvent;
import model.MessageNormalized;
import model.Unknown;

/**
 * Normalizes CDC events by deserializing raw JSON into typed POJOs.
 *
 * Jackson with SNAKE_CASE naming + FAIL_ON_UNKNOWN_PROPERTIES=false handles
 * schema normalization naturally: extra upstream columns are dropped, missing
 * destination columns default to null.
 *
 * All outputs go to side output tags (n known-table tags + 1 unknown tag).
 * Main output is unused.
 */
public class SchemaNormalizer extends KeyedProcessFunction<String, CdcEvent, MessageNormalized> {

    // Per-table side output tags (known tables)
    private final Map<String, OutputTag<MessageNormalized>> tableTags;
    // Side output tag for unknown/drift tables
    private final OutputTag<MessageNormalized> unknownTag;
    // table name -> POJO class for deserialization
    private final Map<String, Class<? extends MessageNormalized>> tableClasses;

    private transient ObjectMapper objectMapper;

    /**
     * @param registry single source of truth for all per-table configuration
     */
    public SchemaNormalizer(TableRegistry registry) {
        this.tableTags = registry.getTableTags();
        this.unknownTag = registry.getUnknownTag();
        this.tableClasses = registry.getTableClasses();
    }

    private ObjectMapper getMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return objectMapper;
    }

    @Override
    public void processElement(CdcEvent event, Context ctx, Collector<MessageNormalized> out) throws Exception {
        String table = event.getTable();
        Class<? extends MessageNormalized> clazz = tableClasses.get(table);
        OutputTag<MessageNormalized> tag = tableTags.get(table);

        if (clazz == null || tag == null) {
            // Unknown table — emit to drift side output for alerting
            Unknown unknown = new Unknown(event.getOp(), table, event.getBefore(), event.getAfter());
            ctx.output(unknownTag, unknown);
            return;
        }

        // Pick the right JSON: after for insert/update, before for delete
        String json = event.isDelete() ? event.getBefore() : event.getAfter();
        if (json == null) {
            return;
        }

        // Jackson deserializes snake_case JSON into camelCase POJO,
        // drops unknown columns, nulls missing columns
        MessageNormalized normalized = getMapper().readValue(json, clazz);
        normalized.setOp(event.getOp());
        normalized.setTable(table);

        ctx.output(tag, normalized);
    }
}
