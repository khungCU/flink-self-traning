package flink.self.traning.models;

public class User {
    public Integer id;
    public String name;
    public String gender;
    public boolean isMembership;

    public User() {}

    public User(Integer id, String name, String gender, boolean isMembership){
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.isMembership = isMembership;
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return this.gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public boolean getIsMembership() {
        return this.isMembership;
    }

    public void setIsMembership(boolean isMembership) {
        this.isMembership = isMembership;
    }





}
