package com.github.reneweb.link

import com.twitter.concurrent.Broker
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Return, Future, Promise}

/**
 * Created by Rene on 23.07.2014.
 */
class LinkClientDispatcher(trans: Transport[PubSub, PubSub])
  extends GenSerialClientDispatcher[PubSub, PubSub, PubSub, PubSub](trans) {
  import GenSerialClientDispatcher.wrapWriteException

  private def handleSubscription(messages: Broker[String], binMessages: Broker[Array[Byte]]) : Future[Unit] = {
    trans.read() flatMap {
      case publish: Publish =>
        publish.message.fold(
          msg => messages.send(msg).sync() flatMap { unit => handleSubscription(messages, binMessages) },
          binMsg => binMessages.send(binMsg).sync() flatMap { unit => handleSubscription(messages, binMessages) }
        )
      case success: PubSubResponseSuccess => handleSubscription(messages, binMessages)
      case error: PubSubResponseError =>
        error.message.map(m =>
          Future.exception(new RuntimeException(m))
        ).getOrElse(Future.exception(new RuntimeException))
      case invalid =>
        Future.exception(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }
  }

  override protected def dispatch(req: PubSub, p: Promise[PubSub]): Future[Unit] = {
    trans.write(req) rescue(wrapWriteException) flatMap {unit =>
      req match {
        case subscribe: Subscribe =>
          val done = new Promise[Unit]
          val messages = new Broker[String]
          val binMessages = new Broker[Array[Byte]]
          handleSubscription(messages, binMessages) ensure(done.setDone())

          val res = SubscribeResponse(subscribe.topic, messages.recv, binMessages.recv)
          p.updateIfEmpty(Return(res))

          done ensure(trans.close())
        case _ =>
          trans.read() flatMap {
            case pubsub: PubSub =>
              p.updateIfEmpty(Return(pubsub))
              Future.Done ensure(trans.close())
            case invalid =>
              Future.exception( new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
          }
      }
    }
  }
}