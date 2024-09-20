package hao.simple.redis.utils.message.queue;

import hao.simple.redis.utils.message.queue.list.Consumer;
import hao.simple.redis.utils.message.queue.list.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class MessageQueueListTest {

    @Test
    public void run() {
        String queue = "test-queue";
        Publisher<Integer> p = new Publisher<>(queue);
        Consumer<Integer> c = new Consumer<>(queue);
        System.out.println(p.publish(1));
        System.out.println(c.consume());
        System.out.println(p.publish(2));
        System.out.println(p.publish(3));
        System.out.println(c.consume());
        System.out.println(c.consume());
    }
}

