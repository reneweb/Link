package com.github.reneweb.link

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.util.Future
import org.jboss.netty.channel._


class LinkServerHandler extends SimpleChannelHandler {
  import LinkServerHandler._

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case publish: Publish =>
        subscriptionsBroker ! publish
        Channels.write(ctx, e.getFuture, new PubSubResponseSuccess(publish.topic))
      case subscribe: Subscribe =>
        subscriptionsBroker ! new Subscription(ctx, e.getFuture, subscribe)
        Channels.write(ctx, e.getFuture, new PubSubResponseSuccess(subscribe.topic))
      case invalid =>
        e.getChannel.write(new PubSubResponseError("", Some("invalid message \"%s\"".format(invalid))))
    }
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
        subscriptions.filter(_.subscribeMsg.topic == publish.topic).map(s => Channels.write(s.ctx, s.channelFuture, publish))
        handleSubscription(subscriptions)
      }
      case unsubscribe: Unsubscribe =>
        val remainingSubs = subscriptions.filterNot(_.channelFuture.getChannel.getId.intValue() == unsubscribe.channelId)
        handleSubscription(remainingSubs)
      case invalid => throw new IllegalArgumentException("invalid message \"%s\"".format(invalid))
    }.sync()
  }

  case class Subscription(ctx: ChannelHandlerContext, channelFuture: ChannelFuture, subscribeMsg: Subscribe) extends BaseMessage
  case class Unsubscribe(channelId: Int) extends BaseMessage
}