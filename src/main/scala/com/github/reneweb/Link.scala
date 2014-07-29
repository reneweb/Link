package com.github.reneweb

import java.net.{SocketAddress, URI}
import java.util.Date

import com.github.reneweb.link._
import com.twitter.finagle._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialServerDispatcher, SerialClientDispatcher}
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.util.{Time, Future}

/**
 * Created by Rene on 17.07.2014.
 */

object LinkListener extends Netty3Listener[PubSub, PubSub](
  "linkListener", LinkCodec().server(ServerCodecConfig("linkServerCodec", new SocketAddress {})).pipelineFactory
)

object LinkServer extends DefaultServer[PubSub, PubSub, PubSub, PubSub](
  "linksrv", LinkListener, new SerialServerDispatcher(_, _)
)

object LinkTransporter extends Netty3Transporter[PubSub, PubSub](
  "linkTransporter", LinkCodec().client(ClientCodecConfig("linkClientCodec")).pipelineFactory
)

object LinkClient extends DefaultClient[PubSub, PubSub](
  name = "linkClient",
  endpointer = Bridge[PubSub, PubSub, PubSub, PubSub](
    LinkTransporter, new LinkClientDispatcher(_)),
  pool = DefaultPool[PubSub, PubSub]()
)

class RichLinkClient(dest: String) {
  val publishService = Link.newClient(dest).toService

  def subscribe(msg: Subscribe): Future[(SubscribeResponse, () => Future[Unit])] = {
    val subscribeClient = Link.newClient(dest).toService

    subscribeClient(msg).map {
      case response: SubscribeResponse => (response, () => subscribeClient.close())
      case invalid => throw new IllegalArgumentException("invalid message \"%s\"".format(invalid))
    }
  }

  def publish(msg: Publish): Future[PubSubResponseSuccess] = {
    publishService(msg).map {
      case response: PubSubResponseSuccess => response
      case invalid => throw new IllegalArgumentException("invalid message \"%s\"".format(invalid))
    }
  }
}

object Link
  extends Client[PubSub, PubSub]
  with Server[PubSub, PubSub]
{
  def newRichClient(dest: String): RichLinkClient = {
    new RichLinkClient(dest)
  }

  def newClient(dest: Name, label: String): ServiceFactory[PubSub, PubSub] =
    LinkClient.newClient(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[PubSub, PubSub]): ListeningServer =
    LinkServer.serve(addr, service)

}

