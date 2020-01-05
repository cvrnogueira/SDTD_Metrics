package io.ensimag.sdtd.metrics

import java.time.{Instant, LocalDate}
import java.util.Arrays

import com.datastax.driver.core.{BoundStatement, Cluster, HostDistance, PoolingOptions, Session}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.collection.convert.ImplicitConversionsToScala._

case class ScatterGraph(location: String, mentions: Long, aqi: Long, updatedAt: Long)

trait CassandraConf {
  val cassandraConf = ConfigFactory.load("cassandra.conf")

  lazy val cassandraKeyspace = cassandraConf.getString("cassandra.keyspace")
  lazy val cassandraHosts = cassandraConf.getString("cassandra.hosts").split(",").toSeq.map(_.trim)
  lazy val cassandraPort = cassandraConf.getInt("cassandra.port")
}

trait CassandraCluster extends CassandraConf with LazyLogging {

  lazy val poolingOptions: PoolingOptions = {
    new PoolingOptions()
      .setConnectionsPerHost(HostDistance.LOCAL, 4, 10)
      .setConnectionsPerHost(HostDistance.REMOTE, 2, 4)
  }

  lazy val cluster: Cluster = {
    val builder = Cluster.builder()
    for (cp <- cassandraHosts) builder.addContactPoint(cp)
    builder.withPort(cassandraPort)
    builder.withPoolingOptions(poolingOptions)

    builder.build()
  }

  lazy implicit val session: Session = cluster.connect()
}

object CassandraOps extends CassandraCluster with LazyLogging {

  lazy val schemaTweetMetadata = "select * from sdtd.twitterpayload where location_sasi"
  lazy val schemaWeatherLocalizedMetadata = "select * from sdtd.weatherpayload where location_sasi like ? limit ?;"
  lazy val schemaWeatherAllMetadata = "select * from sdtd.weatherpayload limit ?;"
  lazy val schemaTweetsSingle = "select * from sdtd.twitterpayload_single where updatedat >= ? and updatedat <= ? limit ? allow filtering;"
  lazy val schemaG8Weather = "select aqi, location, updatedat from sdtd.weatherpayload where location in ?;"
  lazy val topTweetMentions = "select count, location from sdtd.top_twitter_mentions where now = ? order by count desc limit 50;"

  lazy val weatherLocalizedPreparedStatement = session.prepare(schemaWeatherLocalizedMetadata)
  lazy val weatherAllPreparedStatement = session.prepare(schemaWeatherAllMetadata)
  lazy val tweetsSingleAllPreparedStatement = session.prepare(schemaTweetsSingle)
  lazy val g8WeatherStatement = session.prepare(schemaG8Weather)
  lazy val g8WeatherStatementDefault =  g8WeatherStatement.bind(Arrays.asList("united states", "japan", "italy", "united kingdom", "germany", "france", "canada"))
  lazy val topTweetMentionsStatement = session.prepare(topTweetMentions)

  def getTopTweetMentions: List[(String, Long, Long)] = {
    val binding = topTweetMentionsStatement.bind(today.asInstanceOf[Object])

    val now = nowEpoch()

    val fetched = session.execute(binding).map { r =>
      (r.getString("location"), r.getLong("count"), now)
    }
    .toList

    val filtered = fetched
      .groupBy(_._1)
      .toList
      .sortBy(_._2.maxBy(_._2)._2)
      .map(_._2.head)

    val dropN = if (filtered.size > 10) filtered.size - 10 else 0

    filtered.drop(dropN)
  }

  def getG8Weather(location: Option[String]): List[(String, Long, Long)] = {
    val binding = location.map(l => g8WeatherStatement.bind(Arrays.asList(sanitizeLocation(l))))
        .getOrElse(g8WeatherStatementDefault)

    session.execute(binding).map { r =>
      (r.getString("location"), r.getLong("aqi"), r.getLong("updatedat"))
    }.toList
  }

  def getScatterLocationLike(location: Option[String] = None, limit: Int = 25): List[String] = {
    val weatherBindings = location.
      map(loc => weatherLocalizedPreparedStatement.bind(s"%$loc%", limit.asInstanceOf[Object])).
      getOrElse(weatherAllPreparedStatement.bind(limit.asInstanceOf[Object]))

    session.execute(weatherBindings)
      .all()
      .toList
      .map(_.getString("location"))
  }

  def getTweetsPerSec(start: Long, end: Long): List[List[Long]] = {
    val limit = (end - start).toInt

    val bindings = tweetsSingleAllPreparedStatement.bind(
      start.asInstanceOf[Object],
      end.asInstanceOf[Object],
      limit.asInstanceOf[Object]
    )

    session.execute(bindings)
      .all()
      .toList
      .map(r => (r.getLong("updatedat"), r.getLong("mentions")))
      .sortBy(_._1)
      .map({ case (t1, t2) => t2 :: t1 * 1000 :: Nil })
  }

  def getScatterGraphData(start: Long, end: Long, limit: Int, location: Option[String]): List[ScatterGraph] = {

    val weatherBindings = location.
      map(loc => weatherLocalizedPreparedStatement.bind(s"%$loc%", limit.asInstanceOf[Object])).
      getOrElse(weatherAllPreparedStatement.bind(limit.asInstanceOf[Object]))

    val result = session.execute(weatherBindings).all().toList

    result

      .map(r => (r.getLong("aqi"), r.getString("location")))

      // eliminate invalid aqis from air api
      .filter(_._1 != -99)

      .map { t2 =>
        val query = s"$schemaTweetMetadata like '%${t2._2}%' and updatedat >= $start and updatedat <= $end limit $limit allow filtering;"
        val result = session.execute(query).all().toList

        t2.copy(_2 = result)
      }

      .flatMap(t2 => t2._2.map(r =>
        ScatterGraph(
          location = r.getString("location"),
          mentions = r.getLong("mentions"),
          aqi = t2._1,
          updatedAt = r.getLong("updatedat")
        )
      ))
  }

  def sanitizeLocation(value: String) = value.replaceAll("'", "").toLowerCase

  @inline
  def today() = LocalDate.now().toEpochDay

  def nowEpoch() = Instant.EPOCH.toEpochMilli
}