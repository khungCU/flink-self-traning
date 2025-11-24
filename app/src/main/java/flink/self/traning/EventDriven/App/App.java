package flink.self.traning.EventDriven.App;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import EventDriven.SlackSource.SlackMsgModel;
import EventDriven.SlackSource.SlackSource;

public class App {
    public static void main(String[] args) throws Exception {
        String botToken = System.getenv("SLACK_BOT_TOKEN");
        String appToken = System.getenv("SLACK_APP_TOKEN");
        String slackChannelId = System.getenv("SOURCE_CHANNEL_ID");

        System.out.println("=== Starting Slack Bot ===");
        System.out.println("Bot Token: " + (botToken != null ? "SET" : "NOT SET"));
        System.out.println("App Token: " + (appToken != null ? "SET" : "NOT SET"));
        System.out.println("Source Channel ID: " + slackChannelId);
        System.out.println("==========================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use the new Source API with fromSource()
        DataStream<SlackMsgModel> ds = env.fromSource(
                    new SlackSource(botToken, appToken, slackChannelId),
                    WatermarkStrategy.noWatermarks(), // No event time processing
                    "Slack Source")
                .setParallelism(1); // IMPORTANT: Must be 1 for socket connection

        ds.print();

        env.execute("Slack Redirecter");
    }

}
