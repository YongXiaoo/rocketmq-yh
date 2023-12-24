package org.apache.rocketmq.namesrv.xiaoyong;

public class A {
    private final  B b;
    private final String name;
    private final String age;

    public A( String name, String age) {
        this.b = new B(this);
        this.name = name;
        this.age = age;
    }


    public B getB() {
        return b;
    }

    public String getName() {
        return name;
    }

    public String getAge() {
        return age;
    }
}


