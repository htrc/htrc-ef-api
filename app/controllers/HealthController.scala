package controllers

import play.api.mvc.{AbstractController, Action, AnyContent, ControllerComponents}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.DB
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}
import utils.Ping

import javax.inject._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

@Singleton
class HealthController @Inject()(reactiveMongoApi: ReactiveMongoApi, components: ControllerComponents)
                                (implicit val ec: ExecutionContext) extends AbstractController(components) {

  def livenessCheck: Action[AnyContent] = Action { Ok }

  def readinessCheck: Action[AnyContent] =
    Action.async { _ =>
      reactiveMongoApi.database.flatMap(runPing).transform {
        case Success(true) => Success(Ok)
        case _ => Success(ServiceUnavailable)
      }
    }


  implicit val pingCommandWriter: BSONDocumentWriter[Ping.type] =
    BSONDocumentWriter[Ping.type] { _: Ping.type => BSONDocument("ping" -> 1) }

  implicit val pingResultReader: BSONDocumentReader[Boolean] =
    BSONDocumentReader.option[Boolean] { _.booleanLike("ok") }

  private def runPing(db: DB)(implicit ec: ExecutionContext): Future[Boolean] = db.runCommand(Ping)

}
