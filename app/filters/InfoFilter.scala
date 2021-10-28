package filters

import javax.inject._

import akka.stream.Materializer
import play.api.mvc._
import utils.BuildInfo

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class InfoFilter @Inject()(implicit override val mat: Materializer,
                           ec: ExecutionContext) extends Filter {

  override def apply(nextFilter: RequestHeader => Future[Result])
                    (requestHeader: RequestHeader): Future[Result] = {
    val startTime = System.currentTimeMillis

    nextFilter(requestHeader).map { result =>
      val endTime = System.currentTimeMillis
      val responseTime = endTime - startTime

      result.withHeaders(
        "X-EfApi-Version" -> BuildInfo.version,
        "X-EfApi-GitSha" -> BuildInfo.gitSha,
        "X-EfApi-GitBranch" -> BuildInfo.gitBranch,
        "X-EfApi-BuildDate" -> BuildInfo.builtAtString,
        "X-EfApi-ResponseTime" -> responseTime.toString
      )
    }
  }

}
