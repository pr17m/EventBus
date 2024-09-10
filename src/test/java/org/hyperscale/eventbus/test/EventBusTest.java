package org.hyperscale.eventbus.test;

import org.hyperscale.EventBus;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class EventBusTest {

    @Test
    public void test(){
        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            AtomicInteger atomicInteger1 = new AtomicInteger(0);
            AtomicInteger atomicInteger2 = new AtomicInteger(0);
            AtomicInteger atomicInteger3 = new AtomicInteger(0);

            Map<String, Consumer<String>> topicConsumerMap = new HashMap<>();
            topicConsumerMap.put("topic1", t -> atomicInteger1.incrementAndGet());
            topicConsumerMap.put("topic2", t -> atomicInteger2.incrementAndGet());
            topicConsumerMap.put("topic3", t -> atomicInteger3.incrementAndGet());

            EventBus<String> eventBus = new EventBus<>();
            eventBus.init("testBus", 10000, topicConsumerMap, 10000);

            for(int i=0;i<1000000;i++) {
                final int index = i;
                executorService.submit(() -> eventBus.publish("topic"+((index%3)+1), "value"+index));
                executorService.submit(() -> {
                    System.out.println("Atomic1 "+ atomicInteger1.get());
                    System.out.println("Atomic2 "+ atomicInteger2.get());
                    System.out.println("Atomic3 "+ atomicInteger3.get());
                });
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }


    }
}
