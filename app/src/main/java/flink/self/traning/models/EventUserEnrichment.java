package flink.self.traning.models;

public class EventUserEnrichment {
    // User fields
    public Integer id;
    public String userName;
    public String gender;
    public boolean isMembership;

    // Event fields
    public String EventName;
    public Long EventTimestamp;


    EventUserEnrichment(){}

    public EventUserEnrichment(
        Integer id,
        String userName,
        String gender,
        boolean isMembership,
        String EventName,
        Long EventTimestamp
    ){
        this.userName = userName;
        this.gender = gender;
        this.isMembership = isMembership;
        this.EventName = EventName;
        this.EventTimestamp = EventTimestamp;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public boolean isMembership() {
        return isMembership;
    }

    public void setMembership(boolean isMembership) {
        this.isMembership = isMembership;
    }

    public String getEventName() {
        return EventName;
    }

    public void setEventName(String eventName) {
        EventName = eventName;
    }

    public Long getEventTimestamp() {
        return EventTimestamp;
    }

    public void setEventTimestamp(Long eventTimestamp) {
        EventTimestamp = eventTimestamp;
    }

    @Override
    public String toString() {
        return "EventUserEnrichment{" +
                "id=" + id +
                ", userName='" + userName + '\'' +
                ", gender='" + gender + '\'' +
                ", isMembership=" + isMembership +
                ", EventName='" + EventName + '\'' +
                ", EventTimestamp=" + EventTimestamp +
                '}';
    }

}
