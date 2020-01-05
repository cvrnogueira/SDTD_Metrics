package io.ensimag.sdtd.metrics

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.StrictLogging
import spray.json.{DefaultJsonProtocol, JsNumber, JsString, JsValue}

import scala.concurrent.ExecutionContext

trait JsonProtocols extends DefaultJsonProtocol {
  implicit val scatterGraphFormat = jsonFormat4(ScatterGraph)
  implicit val grafanaColumnFormat = jsonFormat2(GrafanaProtocol.Column)
  implicit val grafanaTableResultFormat = jsonFormat2(GrafanaProtocol.TableResult)
  implicit val grafanaQueryRangeFormat = jsonFormat2(GrafanaProtocol.QueryRange)
  implicit val grafanaTagKeyFormat = jsonFormat2(GrafanaProtocol.TagKey)
  implicit val grafanaTagValueRequestFormat = jsonFormat1(GrafanaProtocol.TagValueRequest)
  implicit val grafanaTagValueResponseFormat = jsonFormat1(GrafanaProtocol.TagValueResponse)
  implicit val grafanaQueryScatterFilterFormat = jsonFormat1(GrafanaProtocol.QueryScatterFilter)
  implicit val grafanaQueryScatterTargetFormat = jsonFormat2(GrafanaProtocol.QueryScatterTarget)
  implicit val grafanaSearchRequestFormat = jsonFormat1(GrafanaProtocol.SearchRequest)
  implicit val grafanaSearchResponseFormat = jsonFormat2(GrafanaProtocol.SearchResponse)
  implicit val grafanaAdhocFilterRequestFormat = jsonFormat3(GrafanaProtocol.AdhocFilter)
  implicit val grafanaSeriesResultFormat = jsonFormat2(GrafanaProtocol.SeriesResult)
  implicit val grafanaRequestFormat = jsonFormat4(GrafanaProtocol.QueryRequest)
}

object GrafanaProtocol {

  case class QueryRange(from: String, to: String)

  case class QueryScatterFilter(location: String)

  case class QueryScatterTarget(target: String, data: Option[QueryScatterFilter])

  case class QueryRequest(adhocFilters: List[AdhocFilter], range: QueryRange, maxDataPoints: Int, targets: List[QueryScatterTarget])

  case class SearchRequest(target: String)

  case class SearchResponse(text: String, value: String)

  case class TagValueRequest(key: String)

  case class TagKey(`type`: String, text: String)

  case class TagValueResponse(text: String)

  case class Column(text: String, `type`: String)

  case class AdhocFilter(key: String, operator: String, value: String)

  case class TableResult(columns: List[Column], rows: List[List[JsValue]])

  case class SeriesResult(target: String, datapoints: List[List[Long]])

  val ScatterGraphColumnTypes = List(
    Column("location", "string"),
    Column("mentions", "number"),
    Column("updatedAt", "number"),
    Column("aqi", "number")
  )

  val ScatterTagKeys = TagKey("string", "location") :: Nil

  val EmptyScatterGraphTableResult = TableResult(ScatterGraphColumnTypes, List.empty)
}

class Routes()(implicit val ec: ExecutionContext) extends SprayJsonSupport with JsonProtocols with StrictLogging {

  def route: Route =
    get {

      pathEndOrSingleSlash {
        logger.debug("/ endpoint invoked")

        complete(StatusCodes.OK)
      }

      path("tag-keys") {
        logger.debug("/tag-keys endpoint invoked")

        complete(GrafanaProtocol.ScatterTagKeys)
      }

    } ~ post {

      path("tag-keys") {
        logger.debug("/tag-keys endpoint invoked")

        complete(GrafanaProtocol.ScatterTagKeys)

      } ~ path("tag-values") {
        logger.debug("/tag-values endpoint invoked")

        entity(as[GrafanaProtocol.TagValueRequest]) { tvr =>

          val res = CassandraOps
            .getScatterLocationLike()
            .map(GrafanaProtocol.TagValueResponse)

          complete(res)
        }

      } ~ path("search") {

        entity(as[GrafanaProtocol.SearchRequest]) { gsr =>

          logger.debug("/search endpoint invoked")

          val location = Option(gsr.target)
            .map(_.toLowerCase)
            .filter(_.nonEmpty)

          val res = CassandraOps
            .getScatterLocationLike(location)

          complete(res)
        }

      } ~ path("query") {

        entity(as[GrafanaProtocol.QueryRequest]) { gqr =>

          logger.debug("POST -> /query endpoint invoked")

          // targets come from grafana additional fields
          gqr.targets.headOption.map { data =>

            // make sure we must fetch data to fill scatter graph
            if (data.target == "scatter") {

              complete(
                toGrafanaScatterResponse(
                  CassandraOps.getScatterGraphData(
                    start = Instant.parse(gqr.range.from).toEpochMilli,
                    end = Instant.parse(gqr.range.to).toEpochMilli,
                    limit = gqr.maxDataPoints,
                    location = gqr.adhocFilters.headOption.map(_.value)
                  )
                )
              )

            } else if (data.target == "g8weather") {
              complete(toGrafanaBarsGraphResponse(CassandraOps.getG8Weather(gqr.adhocFilters.headOption.map(_.value))))

            } else if (data.target == "topmentions") {
              complete(toGrafanaBarsGraphResponse(CassandraOps.getTopTweetMentions))

            } else if (data.target == "tweetspersec") {

              complete(
                toGrafanaTweetsPerSecResponse(
                  CassandraOps.getTweetsPerSec(
                    start = Instant.parse(gqr.range.from).getEpochSecond,
                    end = Instant.parse(gqr.range.to).getEpochSecond
                  )
                )
              )

            } else {
              complete(GrafanaProtocol.EmptyScatterGraphTableResult)
            }

          }
          .getOrElse(complete(GrafanaProtocol.EmptyScatterGraphTableResult))
        }
      }
    }

  def toGrafanaBarsGraphResponse(result: List[(String, Long, Long)]): List[GrafanaProtocol.SeriesResult] = {
    result.foldLeft(List.empty[GrafanaProtocol.SeriesResult]) { (acc, r) =>
      val (location, aqi, updatedAt) = r
      val data = (aqi :: updatedAt :: Nil) :: Nil
      GrafanaProtocol.SeriesResult (location, data) :: acc
    }
  }

  def toGrafanaTweetsPerSecResponse(result: List[List[Long]]): List[GrafanaProtocol.SeriesResult] = {
    GrafanaProtocol.SeriesResult("tweetspersec", result) :: Nil
  }

  def toGrafanaScatterResponse(result: List[ScatterGraph]): List[GrafanaProtocol.TableResult] = {

    val rows = result.map { r =>
      List(JsString(r.location), JsNumber(r.mentions), JsNumber(r.updatedAt), JsNumber(r.aqi))
    }

    GrafanaProtocol.TableResult(GrafanaProtocol.ScatterGraphColumnTypes, rows) :: Nil
  }
}