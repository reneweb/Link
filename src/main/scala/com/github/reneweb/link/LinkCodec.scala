package com.github.reneweb.link

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext, ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

case class LinkCodec() extends CodecFactory[PubSub, PubSub] {
  def server = Function.const {
    new Codec[PubSub, PubSub] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("decoder", new LinkDecoder)
          pipeline.addLast("encoder", new LinkEncoder)
          pipeline.addLast("handler", new LinkServerHandler)
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[PubSub, PubSub] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("decoder", new LinkDecoder)
          pipeline.addLast("encoder", new LinkEncoder)
          pipeline.addLast("handler", new LinkClientHandler)
          pipeline
        }
      }
    }
  }
}

class LinkDecoder() extends FrameDecoder {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = ???
}

class LinkEncoder() extends OneToOneEncoder {
  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = ???
}
