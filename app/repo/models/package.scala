package repo
import play.api.libs.json.{Format, Json}
import reactivemongo.api.bson._
package object models {
  type VolumeId = String
  type WorksetId = String
  type IdSet = Set[VolumeId]
  type PageSet = Set[String]

  val idReader: BSONReader[String] = BSONReader.collect[String] {
    case id @ BSONObjectID(_) => id.asInstanceOf[BSONObjectID].stringify
  }

  implicit val worksetHandler: BSONDocumentHandler[Workset] = Macros.handler[Workset]
  implicit val worksetFormat: Format[Workset] = Json.format[Workset]
}
