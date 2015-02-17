package ke.tu.dl.utils

object Cfg {
  import java.util.Properties
  import java.io.FileInputStream

  val cfgBaseDir = "/home/svc_cis4/dl/cfg/"
  val props = loadProperties

  val logBaseDir = props.getProperty("logs.basedir")

  // KAFKA
  val ZOOKEEPER_HOSTS = props.getProperty("hosts.zookeeper")
  val IC_TOPIC = props.getProperty("topic.ic")
  val IC_CONSUMER_GRP = props.getProperty("grp.ic")
  val CI_TOPIC = props.getProperty("topic.ci")
  val CI_CONSUMER_GRP = props.getProperty("grp.ci")
  val SI_TOPIC = props.getProperty("topic.si")
  val SI_CONSUMER_GRP = props.getProperty("grp.si")
  val GI_TOPIC = props.getProperty("topic.gi")
  val GI_CONSUMER_GRP = props.getProperty("grp.gi")
  val BC_TOPIC = props.getProperty("topic.bc")
  val BC_CONSUMER_GRP = props.getProperty("grp.bc")
  val CA_TOPIC = props.getProperty("topic.ca")
  val CA_CONSUMER_GRP = props.getProperty("grp.ca")
  val CR_TOPIC = props.getProperty("topic.cr")
  val CR_CONSUMER_GRP = props.getProperty("grp.cr")
  val FA_TOPIC = props.getProperty("topic.fa")
  val FA_CONSUMER_GRP = props.getProperty("grp.fa")

  // PotgreSQL  DATABASE
  val DB_DRIVER = "org.postgresql.Driver"
  val DB_URL = "jdbc:postgresql://localhost:5432/cis4_dl2"
  val DB_USER = "postgres"
  val DB_PASSWD = "0k5LLO12"
  val DB_POOL_INITSIZE = 20
  val DB_POOL_MAXIDLE = 10

  // SQL Server
  val SQLS_DB_DRIVER = props.getProperty("db.driver")
  val SQLS_DB_URL = props.getProperty("db.url")
  val SQLS_DB_USER = props.getProperty("db.user")
  val SQLS_DB_PASSWD = props.getProperty("db.passwd")

  /**
   * Load properties
   */
  def loadProperties(): Properties = {
    val props = new Properties
    props.load(new FileInputStream(cfgBaseDir + "transformation.properties"))

    val instProps = new Properties
    instProps.load(new FileInputStream(cfgBaseDir + "subscribers.properties"))

    val globalProps = new Properties
    globalProps.load(new FileInputStream(cfgBaseDir + "loader.properties"));

    props.putAll(instProps)
    props.putAll(globalProps)
    return (props)
  }
}

