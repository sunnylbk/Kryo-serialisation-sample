
import akka.actor.ActorSystem
import akka.serialization._
import scala.collection.mutable.AnyRefMap
import scala.collection.mutable.LongMap
import scala.collection.immutable.HashMap
import com.typesafe.config.ConfigFactory

object KryoExample extends App {

  def testConfig(systemName: String, config: String): Unit = {
    val system = ActorSystem("example", ConfigFactory.parseString(config))

    def timeIt[A](name: String, loops: Int)(a: => A) = {
      val now = System.nanoTime
      var i = 0
      while (i < loops) {
        val x = a
        i += 1
      }
      val ms = (System.nanoTime - now) / 1000000
      println(s"$systemName $name:\t$ms\tms\t=\t${loops * 1000 / ms}\tops/s")
    }

    // Get the Serialization Extension
    val serialization = SerializationExtension(system)

    val listLength = 500

    val r = new scala.util.Random(0L)
    val tm = (List.fill(listLength) {
      AnyRefMap[String, Any](
        "foo" -> r.nextDouble,
        "bar" -> "foo,bar,baz",
        "baz" -> 124L,
        "hash" -> HashMap[Int, Int](r.nextInt -> r.nextInt, 5 -> 500, 10 -> r.nextInt))
    }).toArray

    val serialized = serialization.serialize(tm)
    val deserialized = serialization.deserialize(serialized.get, classOf[AnyRefMap[String, Any]])

    println()

  }

  testConfig("LZ4", """
      akka {
        extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension$"]
        actor {
          kryo {
            type = "nograph"
            idstrategy = "incremental"
            kryo-reference-map = false
            buffer-size = 65536
            compression = lz4
            implicit-registration-logging = true
            mappings {
              "scala.collection.immutable.HashMap$HashTrieMap"    = 30
              "[Lscala.collection.immutable.HashMap$HashTrieMap;" = 31
              "scala.collection.mutable.AnyRefMap"                = 34
              "[Lscala.collection.mutable.AnyRefMap;"             = 35
              "scala.collection.mutable.LongMap"                  = 36
              "[Lscala.collection.mutable.LongMap;"               = 37
              "[J" = 50
              "[D" = 51
              "[Z" = 52
              "[Ljava.lang.Object;" = 53
              "[Ljava.lang.String;" = 54
              "scala.math.Ordering$String$" = 100
            }
          }
         serializers {
            kryo = "com.romix.akka.serialization.kryo.KryoSerializer"
          }
          serialization-bindings {
            "scala.collection.mutable.AnyRefMap" = kryo
            "[Lscala.collection.mutable.AnyRefMap;" = kryo
            "scala.collection.mutable.LongMap" = kryo
            "[Lscala.collection.mutable.LongMap;" = kryo
          }
        }
      }
                    """)



}
