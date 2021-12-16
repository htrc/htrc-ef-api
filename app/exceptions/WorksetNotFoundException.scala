package exceptions

case class WorksetNotFoundException(id: String, cause: Throwable = null)
  extends ResourceNotFoundException(s"Workset $id not found", cause)
