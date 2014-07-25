Link
====

Link is a **very** simple PubSub protocol for finagle.

*Note: This is still a work in progress*

##### Usage

```scala
//Create the server... The service doesn't have to do anything, so just return Future.never
val server = Link.serve(":1111", new Service[PubSub, PubSub] {
  override def apply(request: PubSub): Future[PubSub] = Future.never
})

//Create a subscribe client... The subscription will block the connection, thus the service cannot be reused to send other requests
val clientSub = Link.newClient("localhost:1111").toService
clientSub(Subscribe("/test/test")) onSuccess {
  case response: SubscribeResponse => {
    response.out foreach println
  }
  case invalid => //Something went wrong
}

//Create a publish client
val clientPub = Link.newClient("localhost:1111").toService
clientPub(new Publish("/test/test", Left("Some Msg")))

//Close the subscribe client, which will unsubscribe the client from the topic
clientSub.close()
```
