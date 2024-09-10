Caution: I am new to virtual threads and distributed systems, use with caution. Ill try to improve this over time

A highly scalable application level EventBus Written in Java 21 with Virtual Threads.

This uses a push model of submitting the event to subscriber. 

It guarantees the order of execution per topic per worker if you use only 1 worker per topic
i.e
if an event A is published before event B and event C, event B is picked up only after finishing event A. 


if you just want events to be started in causal order 
i.e 
event A to start before event B and event C, you can use many workers per topic.


This is useful when you want to asynchronously run a task in background.


Instead of depending on 3rd party message queues like kafka,RabbitMq, etc. you can use this light weight eventbus
within two components of a same application. 

Could be useful for system design interviews when preparing for interviews of e-commerce companies like Swiggy,
Zomato, Zepto, Blinkit etc.
