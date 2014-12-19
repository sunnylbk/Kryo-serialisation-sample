
import akka.actor.{ ActorRef, ActorSystem }
import akka.serialization._
import com.romix.akka.serialization.kryo.KryoSerializer
import com.typesafe.config.ConfigFactory
import akka.serialization.Serialization
import scala.collection.immutable.TreeMap
import scala.collection.immutable.HashMap
import com.esotericsoftware.minlog.Log

object KryoWithCompression extends App {

  val system = ActorSystem("example", ConfigFactory.parseString("""
	akka {
	  loggers = ["akka.event.Logging$DefaultLogger"]
	  loglevel = "WARNING"
	}
    akka.actor.serializers {
      kryo = "com.romix.akka.serialization.kryo.KryoSerializer"
    }
    akka.actor.kryo {
      trace = true
      idstrategy = "incremental"
      implicit-registration-logging = true
      compression = off
      mappings {
            "akka.actor.ActorRef" = 20
            "akka.actor.DeadLetterActorRef" = 21
            "scala.collection.immutable.HashMap$HashTrieMap" = 30
            "[Lscala.collection.immutable.HashMap$HashTrieMap;" = 31
            "scala.collection.immutable.TreeMap"                = 32
            "[Lscala.collection.immutable.TreeMap;"             = 33
            "scala.collection.immutable.HashSet$HashTrieSet" = 34
            "scala.collection.immutable.$colon$colon" = 35
            "[J" = 50
            "[D" = 51
            "[Z" = 52
            "[Ljava.lang.Object;" = 53
            "[Ljava.lang.String;" = 54
            "scala.math.Ordering$String$" = 100
      }
    }
    akka.actor.serialization-bindings {
      "scala.Product" = kryo
      "scala.collection.Map" = kryo
      "scala.collection.Set" = kryo
      "scala.collection.generic.MapFactory" = kryo
      "scala.collection.generic.SetFactory" = kryo
      "scala.collection.immutable.HashMap$HashTrieMap" = kryo
      "scala.collection.immutable.HashSet$HashTrieSet" = kryo
      "scala.collection.immutable.TreeMap" = kryo
      "[Ljava.lang.Object;" = kryo
      "akka.actor.ActorRef" = kryo
    }
                                                                """))

  val systemWithCompression = ActorSystem("exampleWithCompression", ConfigFactory.parseString("""
	akka {
	  loggers = ["akka.event.Logging$DefaultLogger"]
	  loglevel = "WARNING"
	}
    akka.actor.serializers {
      kryo = "com.romix.akka.serialization.kryo.KryoSerializer"
    }
    akka.actor.kryo {
      trace = true
      idstrategy = "incremental"
      implicit-registration-logging = true
      compression = lz4
      mappings {
            "akka.actor.ActorRef" = 20
            "akka.actor.DeadLetterActorRef" = 21
            "scala.collection.immutable.HashMap$HashTrieMap" = 30
            "[Lscala.collection.immutable.HashMap$HashTrieMap;" = 31
            "scala.collection.immutable.TreeMap"                = 32
            "[Lscala.collection.immutable.TreeMap;"             = 33
            "scala.collection.immutable.HashSet$HashTrieSet" = 34
            "scala.collection.immutable.$colon$colon" = 35
            "[J" = 50
            "[D" = 51
            "[Z" = 52
            "[Ljava.lang.Object;" = 53
            "[Ljava.lang.String;" = 54
            "scala.math.Ordering$String$" = 100
      }
    }
    akka.actor.serialization-bindings {
      "scala.Product" = kryo
      "scala.collection.Map" = kryo
      "scala.collection.Set" = kryo
      "scala.collection.generic.MapFactory" = kryo
      "scala.collection.generic.SetFactory" = kryo
      "scala.collection.immutable.HashMap$HashTrieMap" = kryo
      "scala.collection.immutable.HashSet$HashTrieSet" = kryo
      "scala.collection.immutable.TreeMap" = kryo
      "[Ljava.lang.Object;" = kryo
      "akka.actor.ActorRef" = kryo
    }
                                                                                              """))

  // Get the Serialization Extension
  val serialization = SerializationExtension(system)
  val serializationWithCompression = SerializationExtension(systemWithCompression)
  val hugeCollectionSize = 500

  // Long list for testing serializers and compression
  val testList =
    List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40)

  def serializeDeserialize(serialization: Serialization, obj: AnyRef): Int = {
    val serializer = serialization.findSerializerFor(obj)
    println("Object of class " + obj.getClass.getName + " got serializer of class " + serializer.getClass.getName)
    // Check serialization/deserialization
    val serialized = serialization.serialize(obj)

    val deserialized = serialization.deserialize(serialized.get, obj.getClass)

    deserialized.get.equals(obj)
    serialized.get.size
  }

  val uncompressedSize = serializeDeserialize(serialization, testList)
  val compressedSize = serializeDeserialize(serializationWithCompression, testList)

  println("Compressed Size = " + compressedSize)
  println("Non-compressed Size = " + uncompressedSize)
}
