package org.hyperscale;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;


/*
This event bus uses a push model to the subscriber,
Producer just need to publish the message, subscriber will immediately receive the message if subscriber is ready

We can alternately implement pull model for subscriber in next iteration
 */

public final class EventBus<T> {
    // you can change the logger and random number generate according  to you own implementation
    private static final Logger logger = Logger.getLogger(EventBus.class.getName());

    //https://prng.di.unimi.it
    private final RandomGenerator rng = RandomGeneratorFactory.of("Xoroshiro128PlusPlus").create();

    private Map<String, List<LinkedBlockingQueue<T>>> topicQueueMap;
    private Map<String, Consumer<T>> topicConsumerMap;
    private double maxQueueSizePerTopic;

    // this helps to set config later if config not available during construction
    public EventBus(){}

    //use this when you want to give same thread-pool size for all topic consumers
    public void init(String name, int maxQueueSizePerTopic, Map<String, Consumer<T>> topicConsumerMap, int threadpoolSizePerTopic){
        init(name, maxQueueSizePerTopic, topicConsumerMap, threadpoolSizePerTopic, null );
    }


    //use this when you want to give custom thread-pool size per specific selected topics
    public void init(String name, int maxQueueSizePerTopic, Map<String, Consumer<T>> topicConsumerMap, int threadpoolSizePerTopic, Map<String, Integer> specificThreadpoolSizePerTopic){
        if(name.isEmpty()){
            throw new RuntimeException("Event bus should have a name");
        }

        if(topicConsumerMap == null || topicConsumerMap.isEmpty()) {
            throw new RuntimeException("Topics should have consumers, please provide consumers for topics");
        }

        if(maxQueueSizePerTopic <= 0){
            throw new RuntimeException("Threadpool size must be positive");
        }

        this.topicQueueMap = new HashMap<>(topicConsumerMap.size());
        this.topicConsumerMap = new HashMap<>(topicConsumerMap);
        this.maxQueueSizePerTopic = maxQueueSizePerTopic;


        for (String topic : this.topicConsumerMap.keySet()) {
            int threadpoolSize = threadpoolSizePerTopic;

            if (specificThreadpoolSizePerTopic != null && specificThreadpoolSizePerTopic.get(topic) != null && specificThreadpoolSizePerTopic.get(topic) > 0) {
                threadpoolSize = specificThreadpoolSizePerTopic.get(topic);
            }

            List<LinkedBlockingQueue<T>> queues = new ArrayList<>();
            for (int i = 0; i < threadpoolSize; i++) {
                queues.add(new LinkedBlockingQueue<>(maxQueueSizePerTopic * 2));
            }

            /*
                for every topic we have as many queues as threadpoolSize so that each thread can read one queue.
                Each queue has double the size specified using the params
             */
            this.topicQueueMap.put(topic, queues);


            for (int i = 0; i < threadpoolSize; i++) {
                final int index = i;

                /*
                    be careful with this
                    https://stackoverflow.com/questions/78596905/why-virtual-thread-is-not-garbage-collected-when-not-reachable#:~:text=This%20also%20means%20that%20if,never%20be%20interrupted%20or%20unblocked.
                 */
                Thread t = Thread.ofVirtual().unstarted(() -> {
                    while (true) {
                        try {
                            T eventPayload = this.topicQueueMap.get(topic).get(index).take();
                            process(topic, eventPayload);
                        } catch (InterruptedException e) {
                            logger.info(e.getMessage()); //should be error ideally
                        }
                    }
                });


                t.start();
            }

        }
    }

    private void process(String topic, T eventPayload) {
        if(eventPayload != null) {
            try {
                //make sure that we dont have any CPU intensive task in consumer, as it operates on Virtual Thread
                this.topicConsumerMap.get(topic).accept(eventPayload);
            } catch (Exception e){
                throw new RuntimeException(e);
            }

        }
    }

    /*
        if possible use the publish method from a virtual thread,
        as we are rate limiting there is a potential sleep call during peak hours.
        Sleep is a blocking call that allows this virtual thread to be parked aside by carrier thread.
        Carrier thread can schedule another task till sleep is finished utilize the CPU cycles
     */

    public void publish(String topic, T eventPayload) {
        if(this.topicQueueMap == null) {
            throw new RuntimeException("Eventbus is not initialized - call the init method with necessary parameters before publishing the event");
        }

        if(this.topicQueueMap.get(topic) == null) {
            throw new RuntimeException("Topic not found or topic ");
        }

        List<LinkedBlockingQueue<T>> queues = this.topicQueueMap.get(topic);

        int randomIndex = this.rng.nextInt(queues.size());
        LinkedBlockingQueue<T> queue = queues.get(randomIndex);

        applyBackPressureIfRequired(queue.size());
        try {
            queue.put(eventPayload);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void applyBackPressureIfRequired(int currentQueueSize){
        double BACK_PRESSURE_QUEUE_SIZE_THRESHOLD = 0.9;
        if(currentQueueSize > (this.maxQueueSizePerTopic * BACK_PRESSURE_QUEUE_SIZE_THRESHOLD)) {
            double BASE_SLEEP_TIME_FOR_BACK_PRESSURE = 500;
            long sleepTime = (long) (
                    BASE_SLEEP_TIME_FOR_BACK_PRESSURE
                            *
                        (currentQueueSize - (this.maxQueueSizePerTopic * BACK_PRESSURE_QUEUE_SIZE_THRESHOLD))
                            /
                        (this.maxQueueSizePerTopic * 0.1));

            if(sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Exception while applying backpressure "+e);
                }
            }
        }
    }




}
