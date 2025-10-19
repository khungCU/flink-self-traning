package flink.self.traning.models;

public class Client {

    public Integer id;
    public String name;
    public String gender;
    public boolean vip;

    public Client() {}

    public Client(Integer id, String name, String gender, boolean vip){
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.vip = vip;
    }

    public Integer getId(){
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

    public boolean getVip() {
        return this.vip;
    }

    public void setVip(Boolean vip) {
        this.vip = vip;
    }
    
    @Override
    public String toString() {
        return String.format("Client{id=%d, name='%s', gender='%s', vip=%b}",
                id, name, gender, vip);
    }

}
