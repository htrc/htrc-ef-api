package repo

import com.google.inject.ImplementedBy
import play.api.libs.json.JsObject

import scala.concurrent.Future

@ImplementedBy(classOf[EfRepositoryMongoImpl])
trait EfRepository {
  type VolumeId = String
  type WorksetId = String
  type IdSet = Set[VolumeId]
  type PageSet = Set[String]

  def getVolume(id: VolumeId, withPos: Boolean = true, fields: List[String] = List.empty): Future[JsObject]
  def getVolumes(ids: IdSet, withPos: Boolean = true, fields: List[String] = List.empty): Future[List[JsObject]]
  def getVolumePages(id: VolumeId, pageSeqs: Option[PageSet] = None, withPos: Boolean = true, fields: List[String] = List.empty): Future[JsObject]

  def getVolumeMetadata(id: VolumeId, fields: List[String] = List.empty): Future[JsObject]
  def getVolumesMetadata(ids: IdSet, fields: List[String] = List.empty): Future[List[JsObject]]

  def createWorkset(ids: IdSet): Future[WorksetId]
  def deleteWorkset(id: WorksetId): Future[Unit]
  def getWorksetVolumes(id: WorksetId): Future[IdSet]
}
