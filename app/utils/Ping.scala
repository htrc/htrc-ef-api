package utils

import reactivemongo.api.commands.{Command, CommandKind, CommandWithResult}

object Ping extends Command with CommandWithResult[Boolean] {
  val commandKind: CommandKind = new CommandKind("ping")
}

