package hao.simple.redis.utils.distributed.id;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.List;

@Slf4j
public class DistributedID {

    private static final String DEFAULT = "distributed_id";
    private static final String CAS_LUA = """
            if ARGV[1] == redis.call('get', KEYS[1]) then
                return redis.call('set', KEYS[1], ARGV[2], 'xx')
            else
                return nil
            end
            """;
    private volatile boolean isInit = false;
    private final JedisPool pool;

    private final String key;

    public DistributedID(long lastID) {
        this(DEFAULT, lastID);
    }

    public DistributedID(String key, long lastID) {
        this.key = key;
        pool = new JedisPool("localhost", 6379);
        init(lastID);
    }

    public void init(long lastID) {
        if (!isInit) {
            synchronized (this) {
                if (!isInit) {
                    try (Jedis jedis = pool.getResource()) {
                        String idStr = jedis.get(key);
                        String ret;
                        if (idStr == null) {
                            ret = jedis.set(key, String.valueOf(lastID), SetParams.setParams().nx());
                        } else {
                            long M = Long.parseLong(idStr);
                            long N = Math.max(lastID, M);
                            ret = (String) jedis.eval(CAS_LUA, List.of(key), List.of(idStr, String.valueOf(N)));
                        }

                        log.debug("Init DistributedID: {}, prev: {}, last: {}", ret, idStr, lastID);
                    }
                    isInit = true;
                }
            }
        }

    }

    public long nextID() {
        if (!isInit) {
            throw new RuntimeException("DistributedID is not initialized.");
        }
        try (Jedis jedis = pool.getResource()) {
            return jedis.incr(key);
        }
    }

    public void destroy() {
        pool.close();
    }
}

