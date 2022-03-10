package protocol

import play.api.libs.json._
import play.mvc.Http.Status
import play.api.mvc._
import play.api.mvc.Results._


object WrappedResponse {
  case class WrappedResponse(code: Int, message: Option[String], err: Option[JsValue], data: Option[JsValue])

  implicit val wrappedResponseWrites: Writes[WrappedResponse] = Json.writes[WrappedResponse]

  val Empty: Result = Ok(Json.toJson(WrappedResponse(Status.OK, None, None, None)))

  def apply[T,U](data: Option[T], err: Option[U] = None, code: Int = Status.OK, message: Option[String] = None)
                (implicit writesT: Writes[T], writesU: Writes[U]): Result = {
    val wr = WrappedResponse(code, message, err.map(writesU.writes), data.map(writesT.writes))
    Ok(Json.toJson(wr))
  }

  def apply[T](data: T)
              (implicit writes: Writes[T]): Result = {
    val wr = WrappedResponse(Status.OK, None, None, Some(writes.writes(data)))
    Ok(Json.toJson(wr))
  }

  def apply[T](err: T, code: Int, message: String)
              (implicit writes: Writes[T]): Result = {
    val wr = WrappedResponse(code, Some(message), Some(writes.writes(err)), None)
    Ok(Json.toJson(wr))
  }

  def apply(code: Int, message: String): Result = {
    val wr = WrappedResponse(code, Some(message), None, None)
    Ok(Json.toJson(wr))
  }
}
