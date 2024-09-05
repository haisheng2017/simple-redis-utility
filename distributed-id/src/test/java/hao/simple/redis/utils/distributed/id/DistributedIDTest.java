package hao.simple.redis.utils.distributed.id;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Random;

@Slf4j
public class DistributedIDTest {

    @Test
    public void run() {
        var d = new DistributedID(100);
        log.debug("Generated id:{}", d.nextID());
        log.debug("Generated id:{}", d.nextID());
        log.debug("Generated id:{}", d.nextID());
        log.debug("Generated id:{}", d.nextID());
        log.debug("Generated id:{}", d.nextID());
        log.debug("Generated id:{}", d.nextID());
        d.destroy();
    }

    @SneakyThrows
    @Test
    public void mockMultiThread() {
        DistributedID id = new DistributedID(0);
        Thread t1 = new Thread(new TestRunner(id), "t1");
        Thread t2 = new Thread(new TestRunner(id), "t2");
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        id.destroy();
    }

    @SneakyThrows
    @Test
    public void mockMultiProcess() {
        Thread t1 = new Thread(new TestRunner(), "t1");
        t1.start();
        Thread t2 = new Thread(new TestRunner(), "t2");
        t2.start();
        t1.join();
        t2.join();
    }

    @Slf4j
    @NoArgsConstructor
    private static class TestRunner implements Runnable {
        private DistributedID id;
        private int times = 10;

        public TestRunner(DistributedID id) {
            this.id = id;
        }


        @Override
        public void run() {
            boolean isOwner = false;
            if (id == null) {
                id = new DistributedID(new Random().nextInt(10000));
                isOwner = true;
            }
            while (times-- > 0) {
                log.debug("Generated id:{}", id.nextID());
            }
            if (isOwner) {
                id.destroy();
            }
        }
    }
}

