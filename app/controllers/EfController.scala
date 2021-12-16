package controllers

import javax.inject.Inject
import io.swagger.annotations.ApiParam
import play.api.mvc._
import repo.EfRepository
import play.api.libs.json._

import scala.concurrent.ExecutionContext

class EfController @Inject()(efRepository: EfRepository,
                             components: ControllerComponents)
                            (implicit val ec: ExecutionContext) extends AbstractController(components) {
  import efRepository.{VolumeId, WorksetId}

  def getVolume(@ApiParam(value = "HTID of the volume to fetch", required = true) id: VolumeId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumes(ids)
            .map(volumes => Ok(Json.toJson(volumes.headOption)))
      }
    }

  def getVolumeNoPos(@ApiParam(value = "HTID of the volume to fetch", required = true)  id: VolumeId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumesNoPos(ids)
            .map(volumes => Ok(Json.toJson(volumes.headOption)))
      }
    }

  def getVolumeMetadata(@ApiParam(value = "HTID of the volume to fetch", required = true) id: VolumeId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = Set(id)
          efRepository.getVolumesMetadata(ids)
            .map(metadata => Ok(Json.toJson(metadata.headOption)))
      }
    }

  def getVolumePages(@ApiParam(value = "HTID of the volume to fetch", required = true) id: VolumeId,
                     @ApiParam(value = "Comma-separated list of page sequence numbers to fetch") seq: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val seqs = seq.map(_.split(',').toSet)
          efRepository.getVolumePages(id, seqs)
            .map(Ok(_))
      }
    }

  def getVolumePagesNoPos(@ApiParam(value = "HTID of the volume to fetch", required = true) id: VolumeId,
                          @ApiParam(value = "Comma-separated list of page sequence numbers to fetch") seq: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val seqs = seq.map(_.split(',').toSet)
          efRepository.getVolumePagesNoPos(id, seqs)
            .map(Ok(_))
      }
    }

  def createWorkset(): Action[String] =
    Action.async(parse.text) { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = req.body.linesIterator.toSet
          efRepository.createWorkset(ids).map(wid => Created(Json.obj("id" -> wid)))
      }
    }

  def deleteWorkset(wid: WorksetId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository.deleteWorkset(wid).map(_ => NoContent)
      }
    }

  def getWorksetVolumes(wid: WorksetId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorksetVolumes(wid)
            .flatMap(efRepository.getVolumes)
            .map(volumes => Ok(Json.toJson(volumes)))
      }
    }

  def getWorksetVolumesNoPos(wid: WorksetId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorksetVolumes(wid)
            .flatMap(efRepository.getVolumesNoPos)
            .map(volumes => Ok(Json.toJson(volumes)))
      }
    }

  def getWorksetVolumesMetadata(wid: WorksetId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorksetVolumes(wid)
            .flatMap(efRepository.getVolumesMetadata)
            .map(metadata => Ok(Json.toJson(metadata)))
      }
    }
}
