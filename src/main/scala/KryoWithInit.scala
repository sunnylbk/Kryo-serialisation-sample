import akka.actor.ActorSystem
import akka.serialization.{SerializationExtension, Serialization}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.BlowfishSerializer
import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer
import com.typesafe.config.ConfigFactory
import javax.crypto.KeyGenerator

/**
 * Created with IntelliJ IDEA.
 * User: bharath
 * Date: 18/12/14
 * Time: 10:51 PM
 * To change this template use File | Settings | File Templates.
 */

case class Person(name: String, age: Int)

object KryoWithInit extends App {

  val testCryo = new Kryo
  val key = KeyGenerator.getInstance("Blowfish").generateKey().getEncoded()
  testCryo.register(classOf[String], new BlowfishSerializer(new StringSerializer(), key))

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
            "Person" = 101
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
      "Person" = kryo
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
            "Person" = 101
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
      "Person" = kryo
    }
                                                                                              """))

  // Get the Serialization Extension
  val serialization = SerializationExtension(system)
  val serializationWithCompression = SerializationExtension(systemWithCompression)
  val hugeCollectionSize = 500

  val person = Person("John", 20)

  def serializeDeserialize(serialization: Serialization, obj: AnyRef): Int = {
    val serializer = serialization.findSerializerFor(obj)
    println("Object of class " + obj.getClass.getName + " got serializer of class " + serializer.getClass.getName)
    // Check serialization/deserialization
    val serialized = serialization.serialize(obj)

    val deserialized = serialization.deserialize(serialized.get, obj.getClass)

    println("serialised: " + serialized)
    println("deseriaized: " + deserialized.get)
    println("------------------------------------")
    println("Equals: " + obj.equals(deserialized.get))
    println("------------------------------------")
    serialized.get.size
  }

  val testList = List(Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21),
    Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21),
    Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21),
    Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21), Person("John", 21))

  val uncompressedSize = serializeDeserialize(serialization, testList)
  val compressedSize = serializeDeserialize(serializationWithCompression, testList)

  println("Compressed Size = " + compressedSize)
  println("Non-compressed Size = " + uncompressedSize)


}
