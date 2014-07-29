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

    val messageTypeOpt = if(buffer.readableBytes() >= 1) Some(buffer.getUnsignedByte(0)) else Option.empty

    val frameOpt = messageTypeOpt match {
      case Some(0) =>
        for {
          (contentType, topicLength, msgLength) <-
            if (buffer.readableBytes() >= 10) {
                Some(buffer.getUnsignedByte(1),
                  buffer.getInt(2),
                  buffer.getInt(6))
            } else Option.empty;

          (topic, msg) <-
            if (buffer.readableBytes() >= (topicLength + msgLength + 10)) {
                buffer.skipBytes(10)
                Some(getFrame(buffer, topicLength),
                  getFrame(buffer, msgLength))
            } else Option.empty

        } yield (
          if(contentType == 0) {
            Publish(topic.toString(Charset.forName("UTF-8")), Left(msg.toString(Charset.forName("UTF-8"))))
          } else {
            Publish(topic.toString(Charset.forName("UTF-8")), Right(msg.array()))
          }
        )
      case Some(1) =>
        for(
          topicLength <-
            if (buffer.readableBytes() >= 5) Some(buffer.getInt(1))
            else Option.empty;

          topic <-
            if (buffer.readableBytes() >= (topicLength + 5)) {
              buffer.skipBytes(5)
              Some(getFrame(buffer, topicLength))
            } else Option.empty

        ) yield (Subscribe(topic.toString(Charset.forName("UTF-8"))))
      case Some(2) =>
        bufferToTopicWithOptMessage(buffer).map(r => PubSubResponseSuccess(r._1, r._2))
      case Some(3) =>
        bufferToTopicWithOptMessage(buffer).map(r => PubSubResponseError(r._1, r._2))
      case _ => None
    }

    frameOpt.orNull
  }

  def bufferToTopicWithOptMessage(buffer: ChannelBuffer): Option[(String, Option[String])] = {
    for {
      (topicLength, msgLength) <-
        if (buffer.readableBytes() >= 9) {
          Some(buffer.getInt(1),
            buffer.getInt(5))
        } else Option.empty;

      (topic, msgOpt) <-
        if (buffer.readableBytes() >= (topicLength + msgLength + 9)) {
          buffer.skipBytes(9)
          Some(getFrame(buffer, topicLength),
            if(msgLength != 0) Some(getFrame(buffer, msgLength)) else Option.empty)
        } else Option.empty

    } yield {
      (topic.toString(Charset.forName("UTF-8")), msgOpt.map(_.toString(Charset.forName("UTF-8"))))
    }
  }

  def getFrame(buffer: ChannelBuffer, length: Int) = {
    val readerIndex: Int = buffer.readerIndex
    val actualFrameLength: Int = length
    val frame: ChannelBuffer = extractFrame(buffer, readerIndex, actualFrameLength)
    buffer.readerIndex(readerIndex + actualFrameLength)
    frame
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
      case invalid => throw new IllegalArgumentException("invalid message \"%s\"".format(invalid))
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
      contentTypeOpt.map(buffer.writeByte)
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

