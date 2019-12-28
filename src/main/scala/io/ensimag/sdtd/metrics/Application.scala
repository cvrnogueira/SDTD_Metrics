package io.ensimag.sdtd.metrics

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.Try

object Application extends App with ServerHelpers with StrictLogging {

  val startupTime = System.nanoTime

  logger.info("Bootstrapping customer-data application")

  logger.info("Bootstrapping actor system that will serve http requests")

  implicit val system = ActorSystem("application-actor-system")
  implicit val materializer = ActorMaterializer()

  logger.info("Initializing application layers")

  val routes = new Routes()(global)

  logger.info(s"Initializing http server with available configuration")

  // start http server within configuration host and port
  val serverBiding = startServer(routes.route)

  logger.info("Registering jvm hook for gracefully shutdown")

  // register a jvm shutdown hook that will kill our http server on sigkill or sigterm
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      logger.info(s"Shutting down server...")
      Try(serverBiding.flatMap(_.unbind()).onComplete(_ => system.terminate()))
    }
  })

  // log application start elapsed time
  logger.info(s"Application start finished in ${TimeUnit.SECONDS.convert(System.nanoTime - startupTime, TimeUnit.NANOSECONDS)} seconds")
}
