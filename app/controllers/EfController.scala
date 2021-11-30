package controllers

import javax.inject.Inject
import akka.stream.scaladsl.Source
import io.swagger.annotations.ApiParam
import play.api.mvc._
import repo.EfRepository

import scala.concurrent.{ExecutionContext, Future}

class EfController @Inject()(efRepository: EfRepository,
                             components: ControllerComponents)
                            (implicit val ec: ExecutionContext) extends AbstractController(components) {

  def getVolume(@ApiParam(value = "HTID of the volume to fetch", required = true) id: String): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumes(ids)
            .map(publisher => Ok.chunked(Source.fromPublisher(publisher)))
      }
    }

  def getVolumeNoPos(@ApiParam(value = "HTID of the volume to fetch", required = true)  id: String): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumesNoPos(ids)
            .map(publisher => Ok.chunked(Source.fromPublisher(publisher)))
      }
    }

  def getVolumeMetadata(@ApiParam(value = "HTID of the volume to fetch", required = true) id: String): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumesMetadata(ids)
            .map(publisher => Ok.chunked(Source.fromPublisher(publisher)))
      }
    }

  def getVolumePages(@ApiParam(value = "HTID of the volume to fetch", required = true) id: String,
                     @ApiParam(value = "Comma-separated list of page sequence numbers to fetch") seq: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val seqs = seq.map(_.split(',').toSet)
          efRepository.getVolumePages(id, seqs)
            .map(Ok(_))
      }
    }

  def getVolumePagesNoPos(@ApiParam(value = "HTID of the volume to fetch", required = true) id: String,
                          @ApiParam(value = "Comma-separated list of page sequence numbers to fetch") seq: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val seqs = seq.map(_.split(',').toSet)
          efRepository.getVolumePagesNoPos(id, seqs)
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
