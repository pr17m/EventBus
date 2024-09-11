package org.hyperscale;

import java.util.concurrent.*;

//ordered Eventbus will pull model
public class OrderedEventBus<T> {
    private final ConcurrentHashMap<String, BlockingQueue<T>> topicQueueMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ExecutorService> topicExecutors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, BlockingQueue<T>>> subscriberQueues = new ConcurrentHashMap<>();
    public void createTopic(String topic) {
        topicQueueMap.putIfAbsent(topic, new LinkedBlockingQueue<>());
        topicExecutors.putIfAbsent(topic, Executors.newSingleThreadExecutor());
    }

    public void publish(String topic, T eventPayload) {
        ExecutorService executorService = topicExecutors.get(topic);
        if(executorService == null) {
            throw new IllegalArgumentException("Topic does not exist");
        }

        executorService.submit(() -> {
            subscriberQueues.forEach((subscriber, topicMap) -> {
                BlockingQueue<T> subscriberQueue = topicMap.get(topic);
                if(subscriberQueue != null) {
                    try {
                        subscriberQueue.put(eventPayload);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        });
    }

    public void subscribe(String subscriberId, String topic) {
        topicQueueMap.computeIfPresent(topic, (t, queue) -> {
            subscriberQueues.putIfAbsent(subscriberId, new ConcurrentHashMap<>());
            subscriberQueues.get(subscriberId).putIfAbsent(topic, new LinkedBlockingQueue<>());
           return queue;
        });
    }

    public T pull(String subscriberId, String topic){
        ConcurrentHashMap<String, BlockingQueue<T>> subscriberTopicQueue = subscriberQueues.get(subscriberId);
        if(subscriberTopicQueue == null) {
            throw new IllegalArgumentException("Subscriber does not exist");
        }

        BlockingQueue<T> queue = subscriberTopicQueue.get(topic);
        if(queue == null) {
            throw new IllegalArgumentException(String.format("{%s} topic not subscribed by subscriber {%s}", topic, subscriberId));
        }

        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
