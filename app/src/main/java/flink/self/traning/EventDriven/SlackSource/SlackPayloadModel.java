package flink.self.traning.EventDriven.SlackSource;

public class SlackPayloadModel {

    public String channelId;
    public String channelType;
    public String text;

    public SlackPayloadModel() {
    }

    public SlackPayloadModel(String channelId,
                      String channelType,
                      String text){
        this.channelId = channelId;
        this.channelType = channelType;
        this.text = text;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getChannelType() {
        return channelType;
    }

    public void setChannelType(String channelType) {
        this.channelType = channelType;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "SlackPayloadModel{" +
                "channelId='" + channelId + '\'' +
                ", channelType='" + channelType + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}
