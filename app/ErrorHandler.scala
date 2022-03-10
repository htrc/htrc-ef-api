import exceptions.{ApiException, ErrorCodes}
import play.api._
import play.api.http.DefaultHttpErrorHandler
import play.api.mvc._
import play.api.routing.Router
import protocol.WrappedResponse

import javax.inject._
import scala.concurrent._

@Singleton
class ErrorHandler @Inject()(env: Environment,
                             config: Configuration,
                             sourceMapper: OptionalSourceMapper,
                             router: Provider[Router]) extends DefaultHttpErrorHandler(env, config, sourceMapper, router) {
  val logger: Logger = Logger(getClass)

  override def onServerError(request: RequestHeader, exception: Throwable): Future[Result] = {
    logger.error(request.toString(), exception)

    exception match {
      case e: ApiException => Future.successful(WrappedResponse(e.code, e.getMessage))
      case _ => Future.successful(WrappedResponse(ErrorCodes.InternalServerError, exception.toString))
    }
  }
}
