package com.zj.gsonTest;

import com.google.gson.Gson;
import com.zj.kafka.Person;
import org.junit.Test;

public class gsonTest {
    @Test
    public void test1(){
        Person person = new Person("zhangsan", 20, "sz");
        Gson gson = new Gson();
        String s = gson.toJson(person);
        System.out.println(s);
    }
}
