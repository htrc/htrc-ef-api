package repo

import com.google.inject.ImplementedBy
import play.api.libs.json.JsObject

import scala.concurrent.Future

@ImplementedBy(classOf[EfRepositoryMongoImpl])
trait EfRepository {
  type IdSet = Set[String]
  type FieldSet = Set[String]
  type PageSet = Set[String]

  def getVolumes(ids: IdSet): Future[List[JsObject]]
  def getVolumesNoPos(ids: IdSet): Future[List[JsObject]]
  def getVolumesMetadata(ids: IdSet): Future[List[JsObject]]
  def getVolumePages(id: String, pageSeqs: Option[PageSet] = None): Future[JsObject]
  def getVolumePagesNoPos(id: String, pageSeqs: Option[PageSet] = None): Future[JsObject]

  def createWorkset(ids: IdSet): Future[JsObject]
}
