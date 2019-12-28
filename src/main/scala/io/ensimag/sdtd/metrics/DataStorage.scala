package io.ensimag.sdtd.metrics

import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions, Session}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

case class TwitterPayload(location: String, counter: Long, createdAt: Long)

trait CassandraConf {
  val cassandraConf = ConfigFactory.load("cassandra.conf")

  // cassandra config
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

object CassandraOps extends CassandraCluster {

  import scala.collection.convert.ImplicitConversionsToScala._

  lazy val schemaTweetMetadata =
    """
      |SELECT * FROM sdtd.twitterPayload
      |WHERE location = ?
      |LIMIT 100
    """.stripMargin

  lazy val getTweetMetadataStatement = session.prepare(schemaTweetMetadata)

  def getTweetMetadata(location: String = ""): List[TwitterPayload] = {

    val bindings = getTweetMetadataStatement.bind(location)

    val result = session.execute(bindings).all().toList

    result.map { row =>
      TwitterPayload(
        row.getString("location"),
        row.getLong("counter"),
        row.getLong("createdAt")
      )
    }
  }
}