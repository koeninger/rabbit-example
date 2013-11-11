import com.rabbitmq.client._
import scala.util.Random

object Boilerplate {
  val autoAck = true

  val numHashQueues = 2

  def connect = {
    val factory = new ConnectionFactory
    //  factory.setUri("amqp://guest:guest@localhost/")
    val conn = factory.newConnection()
    val channel = conn.createChannel()
    channel.exchangeDeclare("hash-exchange", "x-consistent-hash", true, false, false, null)
    (1 to numHashQueues).foreach { i =>
      channel.queueDeclare("hash-queue" + i, true, false, false, null)
      channel.queueBind("hash-queue" + i, "hash-exchange", "10")
    }
    (conn, channel)
  }
}

object Producer extends App {
  val (conn, channel) = Boilerplate.connect
  channel.basicPublish("hello-exchange", "hello-routingkey", MessageProperties.TEXT_PLAIN, args(0).getBytes)
  channel.close
  conn.close
}

object PushIdProducer extends App {
  val count = 1000000
  val t1 = System.currentTimeMillis
  val (conn, channel) = Boilerplate.connect
  val rand = new Random()

  (1 to count).foreach { i =>
    channel.basicPublish("hash-exchange", i.toString, MessageProperties.TEXT_PLAIN,    "%064x".format(i).getBytes)
  }
  channel.basicPublish("hash-exchange", (count + 1).toString, MessageProperties.TEXT_PLAIN,    "quit".getBytes)
  println("%s messages in %s millis".format(count, System.currentTimeMillis - t1))
  channel.close
  conn.close
}

object Consumer extends App {
  val t1 = System.currentTimeMillis
  val count = new java.util.concurrent.atomic.AtomicInteger(0)
  val qCount = Array.tabulate(Boilerplate.numHashQueues) { i =>
    new java.util.concurrent.atomic.AtomicInteger(0)
  }
  val (conn, channel) = Boilerplate.connect
 
  def report = {
    println("%s messages in %s millis".format(count, System.currentTimeMillis - t1))
    (1 to Boilerplate.numHashQueues).foreach { i =>
      println("q%s %s".format(i, qCount(i - 1)))
    }
  }
  
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run {
      report
    }
  })

  (1 to Boilerplate.numHashQueues).foreach { i =>
    channel.basicConsume("hash-queue" + i, Boilerplate.autoAck, "hash-consumer" + i,
      new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, props: AMQP.BasicProperties, body: Array[Byte]) {
          val deliveryTag = envelope.getDeliveryTag
          val b = new String(body)
          if (b == "quit") {
            report
            if (!Boilerplate.autoAck) channel.basicAck(deliveryTag, false)
          } else {
            count.incrementAndGet
            qCount(i - 1).incrementAndGet
            if (!Boilerplate.autoAck) channel.basicAck(deliveryTag, false)
          }
        }
      })
  }
}
