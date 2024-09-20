package hao.simple.redis.utils.message.queue;

import hao.simple.redis.utils.message.queue.stream.Consumer;
import hao.simple.redis.utils.message.queue.stream.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class MessageQueueStreamTest {

    @Test
    public void run() {
        String queue = "test-stream";
        Publisher<Integer, Integer> p = new Publisher<>(queue);
        Consumer<Integer, Integer> c = new Consumer<>(queue);
        System.out.println(p.publish(1, 1));
        System.out.println(c.consume());
        System.out.println(p.publish(1, 2));
        System.out.println(p.publish(1, 3));
        System.out.println(c.consume());
        System.out.println(c.consume());
    }
}

