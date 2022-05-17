package controllers

import io.swagger.annotations.ApiParam
import play.api.libs.json._
import play.api.mvc._
import protocol.WrappedResponse
import repo.EfRepository
import utils.Helper.tokenize
import utils.IdUtils._

import javax.inject.Inject
import scala.concurrent.ExecutionContext

class EfController @Inject()(efRepository: EfRepository,
                             components: ControllerComponents)
                            (implicit val ec: ExecutionContext) extends AbstractController(components) {
  import efRepository.{VolumeId, WorksetId}

  def getVolume(@ApiParam(value = "the 'clean' HTID of the volume", required = true) cleanId: VolumeId,
                @ApiParam(value = "'true' whether to include part-of-speech information for tokens, " +
                  "'false' otherwise", required = false, defaultValue = "true") pos: Boolean,
                @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val id = uncleanId(cleanId)
          efRepository.getVolume(id, pos, fields.map(tokenize(_)).getOrElse(List.empty)).map(WrappedResponse(_))
      }
    }

  def getVolumeMetadata(@ApiParam(value = "the 'clean' HTID of the volume", required = true) cleanId: VolumeId,
                        @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val id = uncleanId(cleanId)
          efRepository.getVolumeMetadata(id, fields.map(tokenize(_)).getOrElse(List.empty)).map(WrappedResponse(_))
      }
    }

  def getVolumePages(@ApiParam(value = "the 'clean' HTID of the volume", required = true) cleanId: VolumeId,
                     @ApiParam(value = "comma-separated list of page sequence numbers to fetch") seq: Option[String],
                     @ApiParam(value = "'true' whether to include part-of-speech information for tokens, " +
                       "'false' otherwise", required = false, defaultValue = "true") pos: Boolean,
                     @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val id = uncleanId(cleanId)
          val seqs = seq.map(_.split(',').toSet)
          efRepository.getVolumePages(id, seqs, pos, fields.map(tokenize(_)).getOrElse(List.empty)).map(WrappedResponse(_))
      }
    }

  def createWorkset(): Action[String] =
    Action.async(parse.text) { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = req.body.linesIterator.toSet
          efRepository.createWorkset(ids).map(wid => WrappedResponse(Json.obj("id" -> wid)))
      }
    }

  def deleteWorkset(@ApiParam(value = "the workset ID", required = true) wid: WorksetId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository.deleteWorkset(wid).map(_ => WrappedResponse.Empty)
      }
    }

  def getWorksetVolumes(@ApiParam(value = "the workset ID", required = true) wid: WorksetId,
                        @ApiParam(value = "'true' whether to include part-of-speech information for tokens, 'false' otherwise", required = false, defaultValue = "true") pos: Boolean,
                        @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorksetVolumes(wid)
            .flatMap(ids => efRepository.getVolumes(ids, pos, fields.map(tokenize(_)).getOrElse(List.empty)))
            .map(WrappedResponse(_))
      }
    }

  def getWorksetVolumesMetadata(@ApiParam(value = "the workset ID", required = true) wid: WorksetId,
                                @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorksetVolumes(wid)
            .flatMap(efRepository.getVolumesMetadata(_, fields.map(tokenize(_)).getOrElse(List.empty)))
            .map(WrappedResponse(_))
      }
    }
}
