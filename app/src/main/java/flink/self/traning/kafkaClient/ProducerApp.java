package flink.self.traning.kafkaClient;

import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import flink.self.traning.models.Event;

public class ProducerApp {

    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties() {{
            // User-specific properties that you must set
            put(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
            // Fixed properties
            put(KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getCanonicalName());
            put(VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getCanonicalName());
            put(ACKS_CONFIG,                   "all");
        }};

        final String topic = "events.topic";

        final String[] EVENT_NAMES = {
            "page_view", "button_click", "purchase", "add_to_cart",
            "remove_from_cart", "login", "logout", "search"
        };
    

        try (final Producer<String, Event> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final int numMessages = 100;
            for (int i = 0; i < numMessages; i++) {
                String eventName = EVENT_NAMES[rnd.nextInt(EVENT_NAMES.length)];
                Integer userId = rnd.nextInt(1000) + 1; // User IDs from 1 to 1000
                Long timestamp = System.currentTimeMillis();
                Event event = new Event(eventName, userId, timestamp);
                Thread.sleep(1000);

                producer.send(
                        new ProducerRecord<>(topic, eventName, event),
                        (meta, ex) -> {
                            if (ex != null)
                                ex.printStackTrace();
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, eventName, event);
                        });
            }
            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        }

    }
}