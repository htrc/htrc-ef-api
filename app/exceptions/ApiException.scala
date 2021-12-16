package exceptions

class ApiException(message: String, val code: Int = ErrorCodes.GenericApplicationError, cause: Throwable = null)
  extends Exception(message, cause)

object ApiException {
  def apply(message: String, code: Int = ErrorCodes.GenericApplicationError, cause: Throwable = null): ApiException =
    new ApiException(message, code, cause)
}