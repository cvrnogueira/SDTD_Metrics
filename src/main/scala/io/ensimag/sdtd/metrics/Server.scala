package io.ensimag.sdtd.metrics

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

trait ServerHandlers {
  // customize akka's rejection handler with our http status codes and messages
  implicit val handler = RejectionHandler.newBuilder().handle(handleCommon())
                                                      .handleNotFound { complete(NotFound) }
                                                      .result()

  def handleCommon(): PartialFunction[Rejection, Route] = {
    case ValidationRejection(msg, _)              => complete(UnprocessableEntity)
    case MalformedRequestContentRejection(msg, _) => complete(BadRequest)
  }
}

// methods for starting and configuring application logging level at server startp
trait ServerHelpers extends StrictLogging {

  // find dispatchers (execution contexts) that will be backed by thread-pools
  def lookupDispatchers(system: ActorSystem, name: String): ExecutionContext =
    system.dispatchers.lookup("application.dispatchers." + name)


  // start http server binding host and port as configured within our distributed config store
  def startServer(routes: Route)(implicit ac: ActorSystem, m: ActorMaterializer): Future[ServerBinding] = {

    // start server returning a future bind enabling gracefully shutdown
    val serverBinding = Http().bindAndHandle(routes, "0.0.0.0", 8080)

    logger.info(s"Server up and running at interface 0.0.0.0 and port 8080")

    // return server binding
    serverBinding
  }
}