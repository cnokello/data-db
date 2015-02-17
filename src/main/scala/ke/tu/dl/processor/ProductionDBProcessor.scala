package ke.tu.dl.processor

import kafka.message._
import kafka.consumer._

import org.slf4j.LoggerFactory

import scala.collection.mutable.Map

import org.json4s.{ DefaultFormats, Formats }
import org.json4s.native.JsonParser.parse

import ke.tu.dl.utils.Cfg._

/**
 * Connects to Kafka's IC topic and consume messages by streaming
 */
class ProductionDBProcessor(topic: String, consumerGroup: String, zookeeperHosts: String) {
  import org.apache.commons.io.FileUtils
  import java.io.File
  import ke.tu.dl.localmodel.ContactQuery._
  import ke.tu.dl.localmodel.ContactPersist._
  import ke.tu.dl.localmodel.ContactUtils._
  import ke.tu.dl.localmodel.QContact
  implicit lazy val formats = DefaultFormats

  val logger = LoggerFactory.getLogger(getClass)
  val kafkaConsumer = new KafkaConsumer(topic, consumerGroup, zookeeperHosts)

  def exec(binaryObject: Array[Byte]) = {
    val msg = new String(binaryObject)
    val json = parse(msg)
    val msgMap = json.extract[scala.collection.Map[String, Object]]

    val accountNumber = msgMap.get("accountNumber").getOrElse("").asInstanceOf[String]
    val fileName = msgMap.get("fileName").getOrElse("").asInstanceOf[String]
    val record = msgMap.get("record").getOrElse("").asInstanceOf[String]
    val validRecord = msgMap.get("valid").getOrElse("0").asInstanceOf[String]
    val kbaValidRecord = msgMap.get("kbaValid").getOrElse("0").asInstanceOf[String]
    val missingMandatory = msgMap.get("missingMandatory").getOrElse("0").asInstanceOf[String]
    val validationErrors: List[String] = msgMap.get("errors").getOrElse(null).asInstanceOf[List[String]]
    val recordArr = record.split("\\|", -1)

    var subscriberId: Int = 0
    try {
      val bankCode = fileName.split("\\.").last
      subscriberId = Integer.parseInt(props.getProperty("ke." + bankCode.toUpperCase))
    } catch { case e: Exception => }

    // Save consumer contact, identification and account details
    if (validRecord equals "1")
      try
        if (topic equals IC_TOPIC) {
          val fullName = recordArr(0) + " " + recordArr(1) + " " + recordArr(2) + " " + recordArr(3)
          val ics = searchContact(false, recordArr(12), "001", fullName)
          if (ics.size == 1) {
            val ic = ics(0)
            val ai = getAccountSummary(ic.crn, accountNumber.trim, subscriberId).getOrElse(null)
            if (ai == null) createAccountSummary(toPAccountSummary(recordArr, ic.crn, subscriberId, false))
            else updateAccountSummary(ai)

          } else if (ics.size == 0) {
            val generatedCrn = createContact(toPContact(recordArr, false))
            val summaryId = createAccountSummary(toPAccountSummary(recordArr, generatedCrn, subscriberId, false))
            createAccount(toPAccount(recordArr, generatedCrn, subscriberId, false, 0, summaryId))
            createIdentification(toPIdentification(recordArr, generatedCrn, 0, subscriberId, false))
          }

        } else if (topic equals CI_TOPIC) {
          val registeredName = recordArr(0)
          val cis = searchContact(true, recordArr(3), "005", registeredName)
          if (cis.size == 1) {
            val ci = cis(0)
            val ai = getAccountSummary(ci.crn, accountNumber.trim, subscriberId).getOrElse(null)
            if (ai == null) createAccountSummary(toPAccountSummary(recordArr, ci.crn, subscriberId, false))
            else updateAccountSummary(ai)

          } else if (cis == 0) {
            val generatedCrn = createContact(toPContact(recordArr, true))
            val summaryId = createAccountSummary(toPAccountSummary(recordArr, generatedCrn, subscriberId, true))
            createAccount(toPAccount(recordArr, generatedCrn, subscriberId, true, 0, summaryId))
            createIdentification(toPIdentification(recordArr, generatedCrn, 0, subscriberId, true))
          }
        }
      catch {
        case e: Exception => FileUtils.writeStringToFile(
          new File(logBaseDir + "dl-loader.log"), e.getMessage() + "\n" + e.getStackTraceString + "\n\n", true)
      }
  }

  def read = {
    if (topic equals topic) {
      kafkaConsumer.read(exec)
    }
  }
}