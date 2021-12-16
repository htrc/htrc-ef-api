package exceptions

class BadRequestException(message: String, cause: Throwable = null)
  extends ApiException(message, ErrorCodes.BadRequest, cause)

object BadRequestException {
  def apply(message: String, cause: Throwable = null): BadRequestException =
    new BadRequestException(message, cause)
}