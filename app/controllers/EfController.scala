package controllers

import javax.inject.Inject

import akka.stream.scaladsl.{Source, StreamConverters}
import play.api.http.HttpEntity
import play.api.mvc._
import repo.EfRepository

import scala.concurrent.{ExecutionContext, Future}

class EfController @Inject()(efRepository: EfRepository,
                             components: ControllerComponents)
                            (implicit val ec: ExecutionContext) extends AbstractController(components) {

  def getVolume(id: String): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumes(ids)
            .map(publisher => Ok.chunked(Source.fromPublisher(publisher)))
      }
    }

  def getVolumeMetadata(id: String): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumesMetadata(ids)
            .map(publisher => Ok.chunked(Source.fromPublisher(publisher)))
      }
    }

  def getVolumePages(id: String, seq: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val seqs = seq.map(_.split(',').toList)
          efRepository.getVolumePages(id, seqs)
            .map(Ok(_))
      }
    }

//  def getMultiMetadata: Action[String] =
//    Action.async(parse.text) { implicit req =>
//      render.async {
//        case Accepts.Json() =>
//          if (req.body == null || req.body.isEmpty)
//            Future.successful(BadRequest)
//          else {
//            val ids = req.body.split("""[\|\n]""").toSet
//            efRepository.getMetadata(ids).map(publisher =>
//              Ok.chunked(Source.fromPublisher(publisher))
//            )
//          }
//      }
//    }
}
