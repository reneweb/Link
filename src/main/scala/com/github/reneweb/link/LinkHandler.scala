package com.github.reneweb.link

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.util.Future
import org.jboss.netty.channel._


class LinkServerHandler extends SimpleChannelHandler {
  import LinkServerHandler._

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case publish: Publish =>
        subscriptionsBroker ! publish
        e.getChannel.write(new PubSubResponseSuccess(publish.topic))
      case subscribe: Subscribe =>
        subscriptionsBroker ! new Subscription(e.getChannel, subscribe)
        e.getChannel.write(new PubSubResponseSuccess(subscribe.topic))
      case invalid =>
        e.getChannel.write(new PubSubResponseError("", Some("invalid message \"%s\"".format(invalid))))
    }

    super.messageReceived(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    subscriptionsBroker ! new Unsubscribe(e.getChannel.getId)
    super.channelClosed(ctx, e)
  }
}

object LinkServerHandler {
  protected[LinkServerHandler] val subscriptionsBroker = new Broker[BaseMessage]

  handleSubscription(Set())

  private def handleSubscription(subscriptions: Set[Subscription]): Any = {
    subscriptionsBroker.recv {
      case subscription: Subscription => {
        handleSubscription(subscriptions + subscription)
      }
      case publish: Publish => {
        subscriptions.filter(_.subscribeMsg.topic == publish.topic).map(_.channel.write(publish))
        handleSubscription(subscriptions)
      }
      case unsubscribe: Unsubscribe =>
        val remainingSubs = subscriptions.filterNot(_.channel.getId.intValue() == unsubscribe.channelId)
        handleSubscription(remainingSubs)
      case invalid => new IllegalArgumentException("invalid message \"%s\"".format(invalid))
    }.sync()
  }

  case class Subscription(channel: Channel, subscribeMsg: Subscribe) extends BaseMessage
  case class Unsubscribe(channelId: Int) extends BaseMessage
}