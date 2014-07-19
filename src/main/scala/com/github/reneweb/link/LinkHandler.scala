package com.github.reneweb.link

import com.twitter.concurrent.Offer
import com.twitter.util.Try
import org.jboss.netty.channel._

class LinkHandler extends SimpleChannelHandler {

  protected[this] def write(
     ctx: ChannelHandlerContext,
     sock: PubSub,
     ack: Option[Offer[Try[Unit]]] = None) {
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
  }
}

class LinkServerHandler extends LinkHandler {

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {

  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {

  }
}

class LinkClientHandler extends LinkHandler {

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {

  }


  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {

  }
}