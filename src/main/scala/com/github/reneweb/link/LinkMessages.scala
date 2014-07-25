package com.github.reneweb.link

import com.twitter.concurrent.Offer
import com.twitter.util.{Future, Promise}

/**
 * Created by Rene on 17.07.2014.
 */

abstract class BaseMessage()
abstract class PubSub(val topic: String) extends BaseMessage

case class Publish(override val topic: String, message: Either[String, Array[Byte]]) extends PubSub(topic)
case class Subscribe(override val topic: String) extends PubSub(topic)
case class SubscribeResponse(override val topic: String, out: Offer[String], binaryOut : Offer[Array[Byte]]) extends PubSub(topic)
case class PubSubResponseSuccess(override val topic: String, message: Option[String] = None) extends PubSub(topic)
case class PubSubResponseError(override val topic: String, message: Option[String] = None) extends PubSub(topic)

