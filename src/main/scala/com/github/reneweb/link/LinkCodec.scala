package com.github.reneweb.link

import java.nio.charset.Charset

import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{ChannelTransport, Transport}
import com.twitter.finagle.{Service, Codec, CodecFactory}
import com.twitter.util.Closable
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
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
          pipeline
        }
      }

      override def newClientDispatcher(transport: Transport[Any, Any]): Service[PubSub, PubSub] =
        new LinkClientDispatcher(transport.cast[PubSub, PubSub])

      override def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[Any, Any] =
        new ChannelTransport(ch)
    }
  }
}

class LinkDecoder() extends FrameDecoder {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    {readFlag(buffer) match {
      case Some(0) =>
        for(
          contentType <- readFlag(buffer);
          topicLength <- readLengthField(buffer);
          msgLength <- readLengthField(buffer);
          topic <- readMsg(topicLength, buffer);
          msg <- readMsg(msgLength, buffer)
        ) yield (
          if(contentType == 0) {
            Publish(topic.toString(Charset.forName("UTF-8")), Left(msg.toString(Charset.forName("UTF-8"))))
          } else {
            Publish(topic.toString(Charset.forName("UTF-8")), Right(msg.array()))
          }
        )
      case Some(1) =>
        for(
          topicLength <- readLengthField(buffer);
          topic <- readMsg(topicLength, buffer)
        ) yield (Subscribe(topic.toString(Charset.forName("UTF-8"))))
      case Some(2) =>
        bufferToTopicWithOptMessage(buffer).map(r => PubSubResponseSuccess(r._1, r._2))
      case Some(3) =>
        bufferToTopicWithOptMessage(buffer).map(r => PubSubResponseError(r._1, r._2))
      case _ => None
    }}.getOrElse(null)
  }

  def bufferToTopicWithOptMessage(buffer: ChannelBuffer): Option[(String, Option[String])] = {
    for(
      topicLength <- readLengthField(buffer);
      msgLength <- readLengthField(buffer);
      topic <- readMsg(topicLength, buffer)
    ) yield {
      if(msgLength == 0) (topic.toString(Charset.forName("UTF-8")), None)
      else {
        val topicString = topic.toString(Charset.forName("UTF-8"))
        readMsg(msgLength, buffer).map{
          msg => (topicString, Some(msg.toString(Charset.forName("UTF-8"))))
        }.getOrElse(topicString, None)
      }
    }
  }

  def readFlag(buffer: ChannelBuffer): Option[Int] = {
    if(buffer.readableBytes() < 1) {
      buffer.resetReaderIndex()
      None
    } else {
      buffer.markReaderIndex()
      Some(buffer.readByte().toInt)
    }
  }

  def readLengthField(buffer: ChannelBuffer): Option[Int] = {
    if(buffer.readableBytes() < 4) {
      buffer.resetReaderIndex()
      None
    } else {
      buffer.markReaderIndex()
      Some(buffer.readInt())
    }
  }

  def readMsg(length: Int, buffer: ChannelBuffer): Option[ChannelBuffer] = {
    if(buffer.readableBytes() < length) {
      buffer.resetReaderIndex()
      None
    } else {
      buffer.markReaderIndex()
      Some(buffer.readBytes(length))
    }
  }
}

class LinkEncoder() extends OneToOneEncoder {

  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): ChannelBuffer = {
    msg match {
      case publish: Publish =>
        publish.message.fold(
          msg => topicWithMessageToBuffer(0, publish.topic, Some(0), Some(msg.getBytes("UTF-8"))),
          msgBinary => topicWithMessageToBuffer(0, publish.topic, Some(1), Some(msgBinary))
        )
      case subscribe: Subscribe => topicToBuffer(1, subscribe.topic)
      case response: PubSubResponseSuccess => topicWithMessageToBuffer(2, response.topic, None, response.message.map(_.getBytes("UTF-8")))
      case response: PubSubResponseError => topicWithMessageToBuffer(3, response.topic, None, response.message.map(_.getBytes("UTF-8")))
      case _ => ???
    }
  }

  def topicToBuffer(messageTypeIndex: Int, topic: String): ChannelBuffer = {
    val topicBytes = topic.getBytes("UTF-8")
    val buffer = ChannelBuffers.buffer(1 + 4 + topicBytes.length)

    buffer.writeByte(messageTypeIndex)
    buffer.writeInt(topicBytes.length)
    buffer.writeBytes(topicBytes)
    buffer
  }

  def topicWithMessageToBuffer(messageType: Int, topic: String, contentTypeOpt: Option[Int], messageOpt: Option[Array[Byte]]): ChannelBuffer = {
    val topicBytes = topic.getBytes("UTF-8")

    messageOpt.map { message =>
      val bufferSize = contentTypeOpt.map(_ => 1 + 1 + 4 + 4 + topicBytes.length + message.length)
        .getOrElse(1 + 4 + 4 + topicBytes.length + message.length)
      val buffer = ChannelBuffers.buffer(bufferSize)

      buffer.writeByte(messageType)
      contentTypeOpt.map(buffer.writeByte(_))
      buffer.writeInt(topicBytes.length)
      buffer.writeInt(message.length)
      buffer.writeBytes(topicBytes)
      buffer.writeBytes(message)
      buffer
    }.getOrElse {
      val buffer = ChannelBuffers.buffer(1 + 4 + 4 + topicBytes.length)

      buffer.writeByte(messageType)
      buffer.writeInt(topicBytes.length)
      buffer.writeInt(0)
      buffer.writeBytes(topicBytes)
      buffer
    }
  }

}

