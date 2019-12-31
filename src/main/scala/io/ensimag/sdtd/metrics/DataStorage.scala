package io.ensimag.sdtd.metrics

import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions, Session}
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

  lazy val schemaTweetsSingle = "select * from sdtd.twitterpayload_single where createdat >= ? and createdat <= ? limit ? allow filtering;"
  lazy val schemaTweetsSingleLocalized = "select * from sdtd.twitterpayload_single where createdat >= ? and createdat <= ? and location_sasi like ? limit ? allow filtering;"


  lazy val weatherLocalizedPreparedStatement = session.prepare(schemaWeatherLocalizedMetadata)
  lazy val weatherAllPreparedStatement = session.prepare(schemaWeatherAllMetadata)
  lazy val tweetsSingleLocalizedPreparedStatement = session.prepare(schemaTweetsSingleLocalized)
  lazy val tweetsSingleAllPreparedStatement = session.prepare(schemaTweetsSingle)

  def getScatterLocationLike(location: Option[String] = None, limit: Int = 25): List[String] = {
    val weatherBindings = location.
      map(loc => weatherLocalizedPreparedStatement.bind(s"%$loc%", limit.asInstanceOf[Object])).
      getOrElse(weatherAllPreparedStatement.bind(limit.asInstanceOf[Object]))

    session.execute(weatherBindings)
      .all()
      .toList
      .map(_.getString("location"))
  }

  def getTweetsPerSec(start: Long, end: Long, limit: Int = 25, location: Option[String]): List[List[Long]] = {
    val bindings = location.
      map(loc => tweetsSingleLocalizedPreparedStatement.bind(start.asInstanceOf[Object], end.asInstanceOf[Object], s"%$loc%", limit.asInstanceOf[Object])).
      getOrElse(tweetsSingleAllPreparedStatement.bind(start.asInstanceOf[Object], end.asInstanceOf[Object], limit.asInstanceOf[Object]))

    session.execute(bindings)
      .all()
      .toList
      .map(_.getLong("createdat"))
      .groupBy(_ / 1000)
      .map(t => List(t._2.length, t._1 * 1000))
      .toList
      .sortBy(_.tail.headOption.getOrElse(-1L))
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
}