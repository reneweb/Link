import java.net.{InetSocketAddress, InetAddress}

import com.github.reneweb.Link
import com.github.reneweb.link._
import com.twitter.concurrent.Offer
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ServerBuilder, ClientBuilder}
import com.twitter.util.{Await, Future, RandomSocket}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest._
import scala.util.Random

/**
 * Created by Rene on 22.07.2014.
 */
class LinkSpec extends FlatSpec {

  "Server" should "subscribe client on subscribe msg" in {
    val server = Link.serve(":2345", new Service[PubSub, PubSub] {
      override def apply(request: PubSub): Future[PubSub] = Future.never
    })

    val client = Link.newClient("localhost:2345").toService
    val resultFuture = client(Subscribe("/test/test"))
    val result = Await.result(resultFuture)
    result shouldBe a [SubscribeResponse]
    server.close()
  }

  "Server" should "unsubscribe client on closing of service" in {
    val server = Link.serve(":2346", new Service[PubSub, PubSub] {
      override def apply(request: PubSub): Future[PubSub] = Future.value(request)
    })

    val subService = Link.newClient("localhost:2346").toService
    val subscribeResFuture = subService(Subscribe("/test/test"))
    val subscribeRes = Await.result(subscribeResFuture)
    subscribeRes shouldBe a[SubscribeResponse]

    subService.close()
    server.close()
  }

  "Client" should "receive publish msg, on subscribed topics where a client sends a publish" in {
    val testMessage = "TestMessage"
    val testMessage2 = "TestMessage2"

    //Create server
    val service = new Service[PubSub, PubSub] {
      override def apply(request: PubSub): Future[PubSub] = Future.value(request)
    }

    val server = ServerBuilder()
      .codec(LinkCodec())
      .bindTo(new InetSocketAddress(2347))
      .name("LinkServer")
      .build(service)

    //Create clients
    val clientSub = Link.newClient("localhost:2347").toService
    val subscribeResFuture = clientSub(Subscribe("/test/test"))
    val subscribeRes = Await.result(subscribeResFuture)
    subscribeRes shouldBe a[SubscribeResponse]

    val clientSub2 = Link.newClient("localhost:2347").toService
    val subscribeResFuture2 = clientSub2(Subscribe("/test/test"))
    val subscribeRes2 = Await.result(subscribeResFuture2)
    subscribeRes2 shouldBe a[SubscribeResponse]

    val clientPub = Link.newClient("localhost:2347").toService

    (subscribeRes, subscribeRes2) match {
      case (subscribeResponse: SubscribeResponse, subscribeResponse2: SubscribeResponse) =>
        //Prepare for / send out the first publish
        val msgFuture = subscribeResponse.out.sync()
        val msgFuture2 = subscribeResponse2.out.sync()

        val pubResFuture = clientPub(Publish("/test/test", Left(testMessage)))
        val pubRes = Await.result(pubResFuture)
        pubRes shouldBe a [PubSubResponseSuccess]

        val msg = Await.result(msgFuture)
        val msg2 = Await.result(msgFuture2)
        msg should equal(testMessage)
        msg2 should equal(testMessage)

        //Prepare for / send out a second publish
        val newMsgFuture = subscribeResponse.out.sync()
        val newMsgFuture2 = subscribeResponse2.out.sync()

        val pubResFuture2 = clientPub(Publish("/test/test", Left(testMessage2)))
        val pubRes2 = Await.result(pubResFuture2)
        pubRes2 shouldBe a [PubSubResponseSuccess]

        val newMsg = Await.result(newMsgFuture)
        val newMsg2 = Await.result(newMsgFuture2)
        newMsg should equal(testMessage2)
        newMsg2 should equal(testMessage2)
        server.close()
    }
  }

  "Client" should "receive binary messages" in {
    val testBinMessage = new Array[Byte](1000)
    Random.nextBytes(testBinMessage)

    //Create server
    val service = new Service[PubSub, PubSub] {
      override def apply(request: PubSub): Future[PubSub] = Future.value(request)
    }

    val server = ServerBuilder()
      .codec(LinkCodec())
      .bindTo(new InetSocketAddress(2348))
      .name("LinkServer")
      .build(service)

    //Create clients
    val clientSub = Link.newClient("localhost:2348").toService
    val subscribeResFuture = clientSub(Subscribe("/test/test"))
    val subscribeRes = Await.result(subscribeResFuture)
    subscribeRes shouldBe a[SubscribeResponse]

    val clientPub = Link.newClient("localhost:2348").toService

    subscribeRes match {
      case subscribeResponse: SubscribeResponse =>
        //Prepare for / send out the publish
        val msgFuture = subscribeResponse.binaryOut.sync()

        val pubResFuture = clientPub(Publish("/test/test", Right(testBinMessage)))
        val pubRes = Await.result(pubResFuture)
        pubRes shouldBe a [PubSubResponseSuccess]

        val msg = Await.result(msgFuture)
        msg should equal(testBinMessage)

        server.close()
    }
  }
}
