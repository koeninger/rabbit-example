rabbit-example
==============
Example of durable message queue using RabbitMQ

brew install rabbitmq

/usr/local/Cellar/rabbitmq/*/sbin/rabbitmq-server

sbt 'run-main PushIdProducer'

sbt 'run-main Consumer'
