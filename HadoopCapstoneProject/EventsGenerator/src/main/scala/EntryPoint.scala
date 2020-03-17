import java.net.InetSocketAddress
import java.nio.CharBuffer
import java.nio.channels.SocketChannel
import java.nio.charset.Charset

import org.joda.time.DateTime

import scala.io.Source
import scala.util.Random


case class Event(productName: String,
                 productCategory: String,
                 productPrice: Double,
                 purchaseDate: DateTime,
                 clientIP: String) {
  def toCSV: String = {
    List(productName, productCategory, productPrice, purchaseDate, clientIP).mkString(",")
  }
}

case class JobConfig(productsFile: String,
                     categoriesFile: String,
                     priceMean: Int,
                     priceSd: Int,
                     timeMeanInSec: Int,
                     timeSdInSec: Int,
                     daysOffset: Int,
                     startDate: DateTime,
                     host: String,
                     port: Int,
                     amount: Int,
                     intervalMs: Int)

object JobConfig {
  def parseArgs(args: Array[String]): JobConfig = {
    var productFile: String = "products.txt"
    var categoriesFile: String = "categories.txt"
    val daysOffset = 7
    var startDate = DateTime.now().minusDays(daysOffset)
    val priceMean = 1000
    val priceSd = 100
    val timeMean: Int = 12 * 60 * 60
    val timeSd: Int = 1 * 60 * 60
    var host = "localhost"
    var port = 60083
    var amount = 100000
    var interval = 50

    @scala.annotation.tailrec
    def internalLoop(argsList: List[String]): Unit = {
      argsList match {
        case "--productFile" :: value :: tail =>
          productFile = value
          internalLoop(tail)
        case "--categoriesFile" :: value :: tail =>
          categoriesFile = value
          internalLoop(tail)
        case "--startDate" :: value :: tail =>
          startDate = DateTime.parse(value)
          internalLoop(tail)
        case "--host" :: value :: tail =>
          host = value
          internalLoop(tail)
        case "--port" :: value :: tail =>
          port = value.toInt
          internalLoop(tail)
        case "--amount" :: value :: tail =>
          amount = value.toInt
          internalLoop(tail)
        case "--interval" :: value :: tail =>
          interval = value.toInt
          internalLoop(tail)
        case key :: _ :: _ => throw new IllegalArgumentException("unknown key: " + key)
        case _ :: Nil => throw new IllegalArgumentException("should be in [--key value] format")
        case Nil =>
      }
    }

    internalLoop(args.toList)
    JobConfig(productFile, categoriesFile, priceMean, priceSd, timeMean,
      timeSd, daysOffset, startDate, host, port, amount, interval)
  }
}

class EventSender(socket: SocketChannel) {
  def sendEvent(event: Event): Unit = {
    val buffer = CharBuffer.wrap(event.toCSV + '\n')
    while (buffer.hasRemaining)
      socket.write(Charset.defaultCharset.encode(buffer))
  }

  def close(): Unit = {
    socket.close()
  }
}

object EventSender {
  def prepareSender(host: String, port: Int): EventSender = {
    val socket: SocketChannel = SocketChannel.open()
    socket.connect(new InetSocketAddress(host, port))
    new EventSender(socket)
  }
}

object EntryPoint {
  def main(args: Array[String]): Unit = {
    implicit val config: JobConfig = JobConfig.parseArgs(args)
    implicit val random: Random = new Random()
    val eventSender = EventSender.prepareSender(config.host, config.port)

    var count = 0
    while (count < config.amount) {
      val event = generateEvent(loadFile(config.productsFile), loadFile(config.categoriesFile))
      eventSender.sendEvent(event)
      Thread.sleep(config.intervalMs)
      count = count + 1
      if (count % 1000 == 0) println(count)
    }

    eventSender.close()
  }

  def loadFile(fileName: String) : Array[String] = {
    val productSource = Source.fromFile(fileName)
    val products = productSource.getLines().toArray
    productSource.close()
    products
  }

  def generateEvent(products: Array[String],
                    categories: Array[String],
                    )(implicit random: Random, config: JobConfig): Event = {
    val categoriesAmount = categories.length
    val productCategory = categories(random.nextInt(categoriesAmount))
    val productsAmount = products.length
    val productName = products(random.nextInt(productsAmount))
    val productPrice = getGaussian(config.priceMean, config.priceSd)
    val clientIPv4 = genIPv4
    val dateTime = genDate(config.startDate)

    Event(productName, productCategory, productPrice, dateTime, clientIPv4)
  }

  def genDate(startDate: DateTime)(implicit random: Random, config: JobConfig): DateTime = {
    val dayOffset = random.nextInt(config.daysOffset)
    val secInDay = 86400
    val seconds = Math.min(getGaussian(config.timeMeanInSec, config.timeSdInSec).toInt, secInDay)
    startDate.plusDays(dayOffset).plusSeconds(seconds)
  }

  def genIPv4(implicit random: Random): String = {
    val v = Math.abs(random.nextInt())
    val mask = 255
    List(
      v >> 24 & mask,
      v >> 16 & mask,
      v >> 8 & mask,
      v & mask).mkString(".")
  }

  def getGaussian(mean: Double, sd: Double)(implicit random: Random): Double = {
    Math.max(0, random.nextGaussian() * sd + mean)
  }
}
