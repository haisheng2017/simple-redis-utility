package hao.simple.redis.utils.distributed.id;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

@Slf4j
public class DistributedID {

    private static final String DEFAULT = "distributed_id";

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
                        long M = 0;
                        SetParams sp = SetParams.setParams();
                        if (idStr == null) {
                            sp.nx();
                        } else {
                            sp.xx();
                            M = Long.parseLong(idStr);
                        }
                        long N = Math.max(lastID, M);
                        String ret = jedis.set(key, String.valueOf(N), sp);
                        log.debug("Init DistributedID: {}, last: {}", ret, N);
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

