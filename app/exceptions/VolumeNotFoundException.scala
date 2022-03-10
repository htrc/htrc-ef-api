package exceptions

case class VolumeNotFoundException(id: String, cause: Throwable = null)
  extends ResourceNotFoundException(s"Volume $id not found", cause)
