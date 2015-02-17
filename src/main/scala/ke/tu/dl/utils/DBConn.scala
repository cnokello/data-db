package ke.tu.dl.utils

import org.apache.commons.dbcp2.BasicDataSource
import com.mchange.v2.c3p0.ComboPooledDataSource
import scalikejdbc._
import ke.tu.dl.utils.Cfg._

object LocalDBConn {

  /**
   * Database data source
   */
  val dataSource = {
    val ds = new BasicDataSource
    ds.setDriverClassName(DB_DRIVER)
    ds.setUsername(DB_USER)
    ds.setPassword(DB_PASSWD)
    ds.setMaxIdle(DB_POOL_MAXIDLE);
    ds.setInitialSize(DB_POOL_INITSIZE);
    ds.setUrl(DB_URL)
    ds
  }
}

object ProductionDBConn {
  Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  ConnectionPool.add('sqlServer, SQLS_DB_URL, SQLS_DB_USER, SQLS_DB_PASSWD, settings)

  def getDBConn = ConnectionPool('sqlServer).borrow()
}