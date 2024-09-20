package hao.simple.redis.utils.message.queue.stream;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.util.KeyValue;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class Consumer<K extends Serializable, V extends Serializable> {
    private final String queue;
    private final JedisPool pool;

    private volatile StreamEntryID last = new StreamEntryID(0, 0);

    public Consumer(String queue) {
        this(queue, new JedisPool("localhost", 6379));
    }

    public Consumer(String queue, JedisPool pool) {
        this.queue = queue;
        this.pool = pool;
    }

    public KeyValue<K, V> consume() {
        try (Jedis jedis = pool.getResource()) {
            var resp = jedis.xread(XReadParams.xReadParams()
                    .count(1).block(0), Map.of(queue, last));
            var v = resp.get(0).getValue().get(0);
            last = v.getID();
            var p = v.getFields().entrySet().iterator().next();
            return KeyValue.of(
                    SerializationUtils.deserialize(
                            Base64.getDecoder().decode(p.getKey())),
                    SerializationUtils.deserialize(
                            Base64.getDecoder().decode(p.getValue())
                    )
            );
        }
    }
}
