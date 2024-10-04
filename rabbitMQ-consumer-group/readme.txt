showcase for rabbitMQ consumer group, sharding queue as well
https://helix.apache.org/1.4.1-docs/recipes/rabbitmq_consumer_group.html

docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management

what I changed?
1.use queue instead of exchange + queue, because it's more generic.
2.set resource replica as 2, because it can increase the digestion speed.



