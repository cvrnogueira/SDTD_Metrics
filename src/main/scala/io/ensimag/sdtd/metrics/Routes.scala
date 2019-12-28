package io.ensimag.sdtd.metrics

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.StrictLogging
import spray.json.{DefaultJsonProtocol, JsNumber, JsString, JsValue}

import scala.concurrent.ExecutionContext

trait JsonProtocols extends DefaultJsonProtocol {
  implicit val twitterPayloadProtocol = jsonFormat3(TwitterPayload)
  implicit val grafanaColumn = jsonFormat2(GrafanaProtocol.Column)
  implicit val grafanaTableResult = jsonFormat2(GrafanaProtocol.TableResult)
}

object GrafanaProtocol {

  case class Column(text: String, `type`: String)

  case class TableResult(columns: List[Column], rows: List[List[JsValue]])

  val TwitterPayloadColumnTypes = List(
    Column("location", "string"),
    Column("counter", "number"),
    Column("createdAt", "number"),
  )
}

class Routes()(implicit val ec: ExecutionContext) extends SprayJsonSupport with JsonProtocols with StrictLogging {

  def route: Route =

    get {

      pathEndOrSingleSlash {
        logger.debug("/ endpoint invoked")

        complete(StatusCodes.OK)
      }

    } ~ post {

      path("query") {
        logger.debug("POST -> /query endpoint invoked")

        complete(
          toGrafanaResponse(
            CassandraOps.getTweetMetadata("USA")
          )
        )
      }
    }

  def toGrafanaResponse(result: List[TwitterPayload]): List[GrafanaProtocol.TableResult] = {

    val rows = result.map { r =>
      List(JsString(r.location), JsNumber(r.counter), JsNumber(r.createdAt))
    }

    GrafanaProtocol.TableResult(GrafanaProtocol.TwitterPayloadColumnTypes, rows) :: Nil
  }
}