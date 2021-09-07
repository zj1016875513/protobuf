package com.zj.protobufTest;

import com.zj.testProto.VIP;
import org.junit.Test;

public class protoTest {
    @Test
    public void test1(){
        VIP.User.Builder builder = VIP.User.newBuilder();
        builder.setId(1);
        builder.setName("zhangsan-1");
        builder.setAge(20+1);
        builder.setAddress("shenzhen");
        builder.setRegisterTime("2021-01-01");
        VIP.User user = builder.build();
        System.out.println(user);
        byte[] bytes = user.toByteArray();
        for (byte aByte : bytes) {
            System.out.print(aByte);
        }
    }
}
