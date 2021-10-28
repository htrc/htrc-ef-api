package repo

import akka.stream.Materializer
import org.reactivestreams.Publisher
import play.api.Logging
import play.api.libs.json._
import reactivemongo.api.bson.document

import javax.inject.{Inject, Singleton}

// Reactive Mongo imports
import play.modules.reactivemongo.{ReactiveMongoApi, ReactiveMongoComponents}
import reactivemongo.api.bson.collection.BSONCollection

// BSON-JSON conversions/collection
import reactivemongo.akkastream.cursorProducer
import reactivemongo.play.json.compat._
import reactivemongo.play.json.compat.json2bson.{toDocumentReader, toDocumentWriter}

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EfRepositoryMongoImpl @Inject()(val reactiveMongoApi: ReactiveMongoApi)
                                     (implicit ec: ExecutionContext, m: Materializer)
  extends EfRepository with ReactiveMongoComponents with Logging {

  val _ = implicitly[reactivemongo.api.bson.BSONDocumentWriter[JsObject]]

  protected def efCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("ef"))
  protected def featuresCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("features"))
  protected def metadataCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("metadata"))
  protected def pagesCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("pages"))


  override def getVolumes(ids: IdSet, fields: FieldSet): Future[Publisher[JsObject]] = {
    for  { col <- efCol; features <- featuresCol; metadata <- metadataCol; pages <- pagesCol } yield {
      col
        .aggregateWith[JsObject]() { framework =>
          import framework._

          List(
            Match(document("htid" -> document("$in" -> ids))),
            LookupPipeline(
              from = metadata.name,
              let = document("htid" -> "$htid"),
              pipeline = List(
                Match(document("$expr" -> document("$eq" -> List("$htid", """$$htid""")))),
                Project(document("_id" -> 0)),
                ReplaceRootField("metadata")
              ),
              as = "metadata"
            ),
            UnwindField("metadata"),
            LookupPipeline(
              from = features.name,
              let = document("htid" -> "$htid"),
              pipeline = List(
                Match(document("$expr" -> document("$eq" -> List("$htid", """$$htid""")))),
                Project(document("_id" -> 0)),
                ReplaceRootField("features")
              ),
              as = "features"
            ),
            UnwindField("features"),
            LookupPipeline(
              from = pages.name,
              let = document("htid" -> "$htid"),
              pipeline = List(
                Match(document("$expr" -> document("$eq" -> List("$htid", """$$htid""")))),
                Project(document("_id" -> 0)),
                ReplaceRootField("page")
              ),
              as = "features.pages"
            ),
            Project(document("_id" -> 0))
          )
        }
        .documentPublisher()
    }
  }

  override def getVolumesMetadata(ids: IdSet): Future[Publisher[JsObject]] = {
    val query = document("htid" -> document("$in" -> ids))
    val projection = document("_id" -> 0)

    metadataCol
      .map(_.find(query, Some(projection)))
      .map(_.cursor[JsObject]())
      .map(_.documentPublisher())
  }

  override def getVolumePages(id: String, pageSeqs: Option[PageSet] = None): Future[JsObject] = {
    pagesCol.flatMap { col =>
      var query = document("htid" -> id)
      pageSeqs.foreach(seqs => query ++= "page.seq" -> document("$in" -> seqs))

      col
        .aggregateWith[JsObject]() { framework =>
          import framework._

          List(
            Match(query),
            GroupField("htid")(
              "htid" -> FirstField("htid"),
              "pages" -> PushField("page")
            ),
            Project(document("_id" -> 0))
          )
        }
        .head
    }
  }

  override def getPagesTokenCounts(id: String, pageSeqs: Option[PageSet]): Future[JsObject] = {
    ???
  }

  override def createWorkset(ids: IdSet): Future[JsObject] = {
    ???
  }
}
