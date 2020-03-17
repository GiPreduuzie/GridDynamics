import com.gridu.hive.udf.Utilities
import org.scalatest._
import org.scalatest.matchers.should.Matchers

class UdfSpec extends WordSpec with Matchers {

  def intToBits(int: Int): String = {
    def internalRec(x: Int): List[Int] = {
      if (x == 0) Nil else {
        val m = x % 2
        m :: internalRec(x / 2)
      }
    }

    val result = if (int < 0)
      internalRec(-int - 1).map(x => if (x == 1) 0 else 1).mkString("").padTo(32, '1').reverse
    else
      internalRec(int).mkString("").padTo(32, '0').reverse

    result.grouped(8).mkString(".")
  }

  def parseBinaryIp(ip: String): Int = {
    def loop(value: List[Char]): Int = {
      value match {
        case '.' :: tail => loop(tail)
        case '0' :: tail => loop(tail) * 2
        case '1' :: tail => loop(tail) * 2 + 1
        case Nil => 0
      }
    }
    loop(ip.reverse.toList)
  }

  def intToStringIp(ip: Int): String = {
    def loop(ip: Int): List[Int] = {
      if (ip == 0) Nil else (ip & 255) :: loop(ip >>> 8)
    }
    loop(ip).reverse.mkString(".")
  }

  "IP converter" should {
    "convert string IP to int32" in {
      Seq(
        ("00000000.00000000.00000000.00000000", "0.0.0.0/8"),
        ("11111111.11111111.11111111.11111111", "255.255.255.255/8"),
        ("00000000.00000001.00000000.00000001", "0.1.0.1/8"),
        ("00000000.00000000.01111111.01111111", "0.0.127.127/8"),
        ("00000000.00000000.10000000.10000000", "0.0.128.128/8"),
        ("00000000.00000000.01111101.01111101", "0.0.125.125/8"),
        ("00000000.00000000.11111111.11111111", "0.0.255.255/32"),
        ("01111111.00000000.00000000.00000001", "127.0.0.1/32"),
        ("11111111.00000000.00000000.00000000", "255.0.0.0/8"),
        ("01111111.11111111.11111111.11111111", "127.255.255.255/32"),
        ("10000000.00000000.00000000.00000000", "128.0.0.0/32"),
        ("01111111.11111111.11111111.11111111", "127.255.255.255/32")
      )
        .foreach(kv => intToBits(Utilities.extractIp(kv._2)) shouldBe kv._1)
    }

    "throw exception if IP is malformed" in {
      assertThrows[IllegalArgumentException] {
        Utilities.extractIp("256.18.1.0/0")
      }
    }

    "extract network size" in {
      Utilities.getNetworkSize("255.255.255.255/8") shouldBe 8
      Utilities.getNetworkSize("255.255.255.255/32") shouldBe 32
      Utilities.getNetworkSize("255.255.255.255/24") shouldBe 24
    }

    "mask ip" in {
      Seq(
        ("11111111.11111111.11111111.11111111", "11111111.11111111.11111111.11111111", 32),
        ("11111111.11111111.11111111.11111111", "11111111.11111111.11111111.11111110", 31),
        ("11111111.11111111.11111111.11111111", "11111111.11111111.11111111.11111100", 30),
        ("11111111.11111111.11111111.11111111", "11111111.11111111.11111111.00000000", 24),
        ("11111111.11111111.11111111.11111111", "11111111.11111111.11111110.00000000", 23),
        ("11111111.11111111.11111111.11111111", "11111111.11111111.00000000.00000000", 16),
        ("11111111.11111111.11111111.11111111", "00000000.00000000.00000000.00000000", 0),
        ("10100010.10100111.10111111.11111111", "10100010.10100111.00000000.00000000", 16),
        ("10100010.10100111.10111111.11111111", "10100010.10100000.00000000.00000000", 12),
        ("10100010.10100111.10111111.11111111", "10100000.00000000.00000000.00000000", 3)
      )
        .foreach(kvs => parseBinaryIp(kvs._2) shouldBe Utilities.maskIp(intToStringIp(parseBinaryIp(kvs._1)), kvs._3))
    }
  }
}



