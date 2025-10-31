package flink.self.traning.models;

public class Event {
    public String name;
    public Integer userId;
    public Long timestamp;

    public Event(){}

    public Event(String name, Integer userId, Long timestamp){
        this.name = name;
        this.userId = userId;
        this.timestamp = timestamp;
    }

    public String getName(){
        return this.name;
    }
    public void setName(String name){
        this.name = name;
    }

    public Integer getUserId(){
        return this.userId;
    }
    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return "Event( event_name: " + this.name + ", user_id: " + this.userId + ", timestamp: " + this.timestamp  + " )";
    }

    public String toJson() {
        return String.format(
                "{\"name\":\"%s\",\"userId\":%d,\"timestamp\":%d}",
                this.name,
                this.userId,
                this.timestamp
        );
    }

}
