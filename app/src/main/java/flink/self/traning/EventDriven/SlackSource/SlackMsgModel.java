package flink.self.traning.EventDriven.SlackSource;

public class SlackMsgModel {
    public String channelId;
    public String channelType;
	public String payload;
    
    public SlackMsgModel(String channelId, String channelType, String payload){
        this.channelId = channelId;
        this.channelType = channelType;
        this.payload = payload;
    }

    public String getChannelID(){
        return channelId;
    }

    public String getPayload(){
        return payload;
    }

    public String getChannelType(){
        return channelType;
    }

    @Override
    public String toString(){
        return "Channel ID : " + this.getChannelID() + "\n" +
               "Channel Type : " + this.getChannelType() + "\n" +
               "Message : " + this.getPayload()  + "\n";
    }
}
