package co.zs._04serializer;

import java.io.Serializable;
import java.util.Date;

/**
 * 自定义用户
 *
 * @author shuai
 * @date 2020/03/19 13:22
 */
public class User implements Serializable {
    private Integer id;
    private String name;
    private Date birthDay;

    public User() {
    }

    public User(Integer id, String name, Date birthDay) {
        this.id = id;
        this.name = name;
        this.birthDay = birthDay;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getBirthDay() {
        return birthDay;
    }

    public void setBirthDay(Date birthDay) {
        this.birthDay = birthDay;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", birthDay=" + birthDay +
                '}';
    }
}
