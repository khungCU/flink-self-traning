package flink.self.traning.customDataGen;

import flink.self.traning.models.Event;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

public class EventGeneratorFunction implements GeneratorFunction<Long, Event> {

    private static final String[] EVENT_NAMES = {
            "page_view", "button_click", "purchase", "add_to_cart",
            "remove_from_cart", "login", "logout", "search"
    };

    private Random random;

    @Override
    public Event map(Long index) throws Exception {
        // Initialize Random lazily with a unique seed based on index and nanoTime
        // This ensures each parallel instance gets different random sequences
        random = new Random(index * 31L + System.nanoTime());
        

        String eventName = EVENT_NAMES[random.nextInt(EVENT_NAMES.length)];
        Integer userId = random.nextInt(1000) + 1; // User IDs from 1 to 1000
        Long timestamp = System.currentTimeMillis();
        Thread.sleep((random.nextInt(3) + 1) * 1000);
        return new Event(eventName, userId, timestamp);
    }
}