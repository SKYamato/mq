package co.zs.queue._03transaction_object;

import java.io.Serializable;

/**
 * @author shuai
 * @date 2020/03/25 8:56
 */
public class User implements Serializable {
    /**
     * name : zhang
     * age : 18
     * price : 2233.23
     */
    private String name;
    private int age;
    private double price;

    public User(String name, int age, double price) {
        this.name = name;
        this.age = age;
        this.price = price;
    }

    public User() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", price=" + price +
                '}';
    }
}
