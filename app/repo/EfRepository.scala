package repo

import com.google.inject.ImplementedBy
import org.reactivestreams.Publisher
import play.api.libs.json.JsObject

import scala.concurrent.Future

@ImplementedBy(classOf[EfRepositoryMongoImpl])
trait EfRepository {
  type IdSet = Set[String]
  type FieldSet = Set[String]
  type PageSet = Set[String]

  def getVolumes(ids: IdSet): Future[Publisher[JsObject]]
  def getVolumesNoPos(ids: IdSet): Future[Publisher[JsObject]]
  def getVolumesMetadata(ids: IdSet): Future[Publisher[JsObject]]
  def getVolumePages(id: String, pageSeqs: Option[PageSet] = None): Future[JsObject]
  def getVolumePagesNoPos(id: String, pageSeqs: Option[PageSet] = None): Future[JsObject]

  def createWorkset(ids: IdSet): Future[JsObject]
}
