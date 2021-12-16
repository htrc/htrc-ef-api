package exceptions

abstract class ResourceNotFoundException(message: String, cause: Throwable = null)
  extends ApiException(message, ErrorCodes.ResourceNotFound, cause)