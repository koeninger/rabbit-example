import com.rabbitmq.client._

object Boilerplate {
  def connect = {
    val factory = new ConnectionFactory
    //  factory.setUri("amqp://guest:guest@localhost/")
    val conn = factory.newConnection()
    val channel = conn.createChannel()
    channel.exchangeDeclare("hello-exchange", "direct", true, false, false, null)
    channel.queueDeclare("hello-queue", true, false, false, null)
    channel.queueBind("hello-queue", "hello-exchange", "hello-routingkey")
    (conn, channel)
  }
}

object Producer extends App {
  val (conn, channel) = Boilerplate.connect
  channel.basicPublish("hello-exchange", "hello-routingkey", MessageProperties.PERSISTENT_TEXT_PLAIN, args(0).getBytes)
  channel.close
  conn.close
}

object PushIdProducer extends App {
  val count = 1000000
  val t1 = System.currentTimeMillis
  val (conn, channel) = Boilerplate.connect
  (1 to count).foreach { i =>
    channel.basicPublish("hello-exchange", "hello-routingkey", MessageProperties.PERSISTENT_TEXT_PLAIN,    "%064x".format(i).getBytes)
  }
  channel.basicPublish("hello-exchange", "hello-routingkey", MessageProperties.PERSISTENT_TEXT_PLAIN,    "quit".getBytes)
  println("%s messages in %s millis".format(count, System.currentTimeMillis - t1))
  channel.close
  conn.close
}

object Consumer extends App {
  val t1 = System.currentTimeMillis
  val count = new java.util.concurrent.atomic.AtomicInteger(0)
  val (conn, channel) = Boilerplate.connect
 
  def report = println("%s messages in %s millis".format(count, System.currentTimeMillis - t1))
  
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run {
      report
    }
  })

  channel.basicConsume("hello-queue", false, "hello-consumer",
    new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, props: AMQP.BasicProperties, body: Array[Byte]) {
        val deliveryTag = envelope.getDeliveryTag
        val b = new String(body)
        if (b == "quit") {
          report
          channel.basicAck(deliveryTag, false)
        } else {
          count.incrementAndGet
          channel.basicAck(deliveryTag, false)
        }
      }
    })
}
