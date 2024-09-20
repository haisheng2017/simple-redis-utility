package hao.simple.redis.utils.message.queue.list;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@AllArgsConstructor
public class Consumer<T extends Serializable> {
    private final String queue;
    private final boolean ack;
    private final JedisPool pool;

    public Consumer(String queue) {
        this(queue, false, new JedisPool("localhost", 6379));
    }

    public T consume() {
        try (Jedis jedis = pool.getResource()) {
            // queue, ele ...
            var resp = jedis.blpop(0, queue.getBytes(StandardCharsets.UTF_8));
            byte[] ele = resp.get(1);
            return SerializationUtils.deserialize(ele);
        }
    }
}
