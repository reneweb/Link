package com.github.reneweb.link

import com.twitter.concurrent.Offer
import com.twitter.util.{Future, Promise}

/**
 * Created by Rene on 17.07.2014.
 */

abstract case class PubSub(topic: String, onClose: Future[Unit] = new Promise[Unit], close: () => Unit = { () => () })

case class Publish(override val topic: String, message: Offer[String], binaryMessage: Offer[Array[Byte]]) extends PubSub(topic)
case class Subscribe(override val topic: String) extends PubSub(topic)
case class Unsubscribe(override val topic: String) extends PubSub(topic)
case class PubSubResponse(override val topic: String, message: Option[String] = None) extends PubSub(topic)

