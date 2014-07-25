package com.github.reneweb

import java.net.{SocketAddress, URI}

import com.github.reneweb.link.{PubSubResponseError, LinkClientDispatcher, PubSub, LinkCodec}
import com.twitter.finagle._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialServerDispatcher, SerialClientDispatcher}
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._

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

object Link
  extends Client[PubSub, PubSub]
  with Server[PubSub, PubSub]
{
  def newClient(dest: Name, label: String): ServiceFactory[PubSub, PubSub] =
    LinkClient.newClient(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[PubSub, PubSub]): ListeningServer =
    LinkServer.serve(addr, service)

}

