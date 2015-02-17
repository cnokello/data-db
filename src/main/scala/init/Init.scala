package init

import akka.actor.{ Actor, ActorRef, Props, ActorSystem }

import ke.tu.dl.processor.ProductionDBProcessor

case class ProcessStringMsg(string: String)
case class StringProcessedMsg(words: Integer)
case class ProcessIC()
case class ProcessCI()
case class ProcessSI()
case class ProcessGI()
case class ProcessBC()
case class ProcessCA()
case class ProcessCR()
case class ProcessFA()

class StringCounterActor extends Actor {
  def receive = {
    case ProcessStringMsg(string) => {
      val wordsInLine = string.split(" ").length
      sender ! StringProcessedMsg(wordsInLine)
    }
    case _ => println("Error: message not recognized")
  }
}

case class StartProcessFileMsg()

class WordCounterActor(filename: String) extends Actor {
  private var running = false
  private var totalLines = 0
  private var linesProcessed = 0
  private var result = 0
  private var fileSender: Option[ActorRef] = None

  def receive = {

    case StartProcessFileMsg() => {
      if (running) {
        // println used for demo only. Akka logger should be used instead
        println("Warning: duplicate start message received")
      } else {
        running = true
        fileSender = Some(sender) // Save ref to process invoker

        import scala.io.Source._
        fromFile(filename).getLines.foreach {
          line =>
            context.actorOf(Props[StringCounterActor]) ! ProcessStringMsg(line)
            totalLines += 1
        }
      }
    }

    case StringProcessedMsg(words) => {
      result += words
      linesProcessed += 1
      if (linesProcessed == totalLines) {
        fileSender.map(_ ! result) // provide result to process invoker
      }
    }

    case _ => println("message not reconized!")
  }
}

case class PrintMessage(msg: Integer)
case class PrintStrMessage(msg: String)
case class PrintRespMessage(msg: String)

class PrintMsgActor extends Actor {
  def receive = {
    case PrintMessage(msg) => {
      //println("Total: " + msg)
      sender ! "Message from remote agent: Received " + msg
    }
    case PrintStrMessage(msg) => {
      //println(msg)
      sender ! PrintRespMessage("Message from remote agent: Received " + msg)
    }
    case _ => println("Invalid message")
  }
}

class ProcessCAActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system

  def receive = {
    case ProcessCA() => {
      val caProcessor = new ProductionDBProcessor(CA_TOPIC, CA_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      caProcessor.read
    }
    case _ => println("Unrecognized message")
  }
}

class ProcessCRActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system

  def receive = {
    case ProcessCR() => {
      val crProcessor = new ProductionDBProcessor(CR_TOPIC, CR_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      crProcessor.read
    }
    case _ => println("Unrecognized message.")
  }
}

class ProcessICActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system

  def receive = {
    case ProcessIC() => {
      val icProcessor = new ProductionDBProcessor(IC_TOPIC, IC_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      icProcessor.read

    }
    case _ => println("Unrecognized message.")

  }
}

class ProcessCIActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system

  def receive = {
    case ProcessCI() => {
      val ciProcessor = new ProductionDBProcessor(CI_TOPIC, CI_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      ciProcessor.read
    }
    case _ => println("Unrecognized message.")

  }
}

class ProcessSIActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system
  def receive = {
    case ProcessSI() => {
      val siProcessor = new ProductionDBProcessor(SI_TOPIC, SI_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      siProcessor.read
    }
    case _ => println("Unrecognized message.")

  }
}

class ProcessGIActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system

  def receive = {
    case ProcessGI() => {
      val giProcessor = new ProductionDBProcessor(GI_TOPIC, GI_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      giProcessor.read
    }
    case _ => println("Unrecognized message.")

  }
}

class ProcessFAActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system

  def receive = {
    case ProcessFA() => {
      val faProcessor = new ProductionDBProcessor(FA_TOPIC, FA_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      faProcessor.read
    }
    case _ => println("Unrecognized message.")

  }
}

class ProcessBCActor extends Actor {
  import ke.tu.dl.utils.Cfg._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = context.system

  def receive = {
    case ProcessBC() => {
      val bcProcessor = new ProductionDBProcessor(BC_TOPIC, BC_CONSUMER_GRP, ZOOKEEPER_HOSTS)
      bcProcessor.read
    }
    case _ => println("Unrecognized message.")

  }
}

object DLApp extends App {
  import scala.reflect.ScalaSignature
  import scala.concurrent.Future
  import scala.concurrent.Promise
  import akka.util.Timeout
  import scala.concurrent.duration._
  import akka.pattern.ask
  import akka.dispatch.ExecutionContexts._
  import akka.dispatch.Futures
  import scala.concurrent.ExecutionContext.Implicits.global
  import ke.tu.dl.utils.Cfg._
  import ke.tu.dl.processor._

  override def main(args: Array[String]) {

    val system = ActorSystem("RemoteSystem")
    val processICActor = system.actorOf(Props[ProcessICActor], "ProcessICActor")
    val processCIActor = system.actorOf(Props[ProcessCIActor], "ProcessCIActor")
    val processSIActor = system.actorOf(Props[ProcessSIActor], "ProcessSIActor")
    val processGIActor = system.actorOf(Props[ProcessGIActor], "ProcessGIActor")
    val processCAActor = system.actorOf(Props[ProcessCAActor], "ProcessCAActor")
    val processCRActor = system.actorOf(Props[ProcessCRActor], "ProcessCRActor")
    val processBCActor = system.actorOf(Props[ProcessBCActor], "ProcessBCActor")
    val processFAActor = system.actorOf(Props[ProcessFAActor], "ProcessFAActor")

    processICActor ! ProcessIC()
    processCIActor ! ProcessCI()
    processSIActor ! ProcessSI()
    processGIActor ! ProcessGI()
    processCAActor ! ProcessCA()
    processCRActor ! ProcessCR()
    processBCActor ! ProcessBC()
    processFAActor ! ProcessFA()

  }
}