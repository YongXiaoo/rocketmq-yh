package org.apache.rocketmq.namesrv.xiaoyong;

public class TestAB {

    public static void main(String[] args) {
        A youzi = new A("youzi", "18");

        System.out.println(youzi + "B : " + youzi.getB().getA());


    }
}
