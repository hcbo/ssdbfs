package com.hcb;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import org.junit.Test;

public class AppTest {


    @Test
    public void app8() {
        RedisClient redisClient = new RedisClient(
                RedisURI.create("redis://192.168.225.6:8888"));
        RedisConnection redisConnection = redisClient.connect();
        String result = redisConnection.ping();
        System.out.println(result);
    }



}
