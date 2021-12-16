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

  def getVolumes(ids: IdSet): Future[List[JsObject]]
  def getVolumesNoPos(ids: IdSet): Future[List[JsObject]]
  def getVolumesMetadata(ids: IdSet): Future[List[JsObject]]
  def getVolumePages(id: VolumeId, pageSeqs: Option[PageSet] = None): Future[JsObject]
  def getVolumePagesNoPos(id: VolumeId, pageSeqs: Option[PageSet] = None): Future[JsObject]

  def createWorkset(ids: IdSet): Future[WorksetId]
  def deleteWorkset(id: WorksetId): Future[Unit]
  def getWorksetVolumes(id: WorksetId): Future[IdSet]
}
