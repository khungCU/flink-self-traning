package flink.self.training.asyncLookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;


// https://openweathermap.org/current
// https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}
public class AsyncWeather extends RichAsyncFunction<Location, EnrichLocation> {

    private static final String API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather";
    private transient CloseableHttpAsyncClient httpClient;
    private transient ObjectMapper objectMapper;
    private String apiKey;

    public AsyncWeather(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize HTTP async client
        httpClient = HttpAsyncClients.createDefault();
        httpClient.start();

        // Initialize JSON parser
        objectMapper = new ObjectMapper();
    }

    @Override
    public void asyncInvoke(Location input, ResultFuture<EnrichLocation> resultFuture) throws Exception {
        // Build the API URL with query parameters
        String url = String.format("%s?lat=%f&lon=%f&appid=%s&units=metric",
                API_BASE_URL,
                input.getLatitude(),
                input.getLongitude(),
                apiKey);

        HttpGet request = new HttpGet(url);

        // Execute async HTTP request
        httpClient.execute(request, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse response) {
                try {
                    String json = EntityUtils.toString(response.getEntity());
                    JsonNode weatherData = objectMapper.readTree(json);

                    // Extract weather information
                    String weather = weatherData.get("weather").get(0).get("description").asText();
                    double temperature = weatherData.get("main").get("temp").asDouble();
                    double bodyTempature = weatherData.get("main").get("feels_like").asDouble();

                    // Create enriched location with weather data
                    EnrichLocation enriched = new EnrichLocation(
                            input.getLatitude(),
                            input.getLongitude(),
                            input.getCity(),
                            weather,
                            temperature,
                            bodyTempature
                    );

                    resultFuture.complete(Collections.singleton(enriched));
                } catch (Exception e) {
                    resultFuture.completeExceptionally(e);
                }
            }

            @Override
            public void failed(Exception ex) {
                resultFuture.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                resultFuture.completeExceptionally(
                        new Exception("Request cancelled for location: " + input.getCity()));
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
