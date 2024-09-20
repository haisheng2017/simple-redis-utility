package hao.simple.redis.utils.message.queue.stream;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;

import java.io.Serializable;
import java.util.Base64;
import java.util.Map;

@AllArgsConstructor
public class Publisher<K extends Serializable, V extends Serializable> {
    private final String queue;
    private final JedisPool pool;

    public Publisher(String queue) {
        this(queue, new JedisPool("localhost", 6379));
    }

    public String publish(K key, V val) {
        String k = Base64.getEncoder().encodeToString(SerializationUtils.serialize(key));
        String v = Base64.getEncoder().encodeToString(SerializationUtils.serialize(val));
        try (Jedis jedis = pool.getResource()) {
            var resp = jedis.xadd(queue, XAddParams.xAddParams().id(StreamEntryID.NEW_ENTRY), Map.of(k, v));
            return resp.toString();
        }
    }
}
