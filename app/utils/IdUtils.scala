package utils

object IdUtils {
  def uncleanId(id: String): String =
    id.replace('+', ':').replace('=', '/').replace(',', '.')

  def cleanId(id: String): String = {
    def _cleanId(id: String): String = id.replace(':', '+').replace('/', '=').replace('.', ',')
    id match {
      case s"$l.$r" => s"$l.${_cleanId(r)}"
      case _ => throw new IllegalArgumentException(id)
    }
  }
}
