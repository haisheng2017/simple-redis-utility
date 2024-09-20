package hao.simple.redis.utils.message.queue.list;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor
public class Publisher<T extends Serializable> {
    private final String queue;
    private final boolean ack;
    private final JedisPool pool;

    public Publisher(String queue) {
        this(queue, false, new JedisPool("localhost", 6379));
    }

    public int publish(T obj) {
        byte[] msg = SerializationUtils.serialize(obj);
        try (Jedis jedis = pool.getResource()) {
            return (int) jedis.rpush(queue.getBytes(StandardCharsets.UTF_8), msg);
        }
    }
}
