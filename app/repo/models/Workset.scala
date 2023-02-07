package repo.models

import reactivemongo.api.bson.Macros.Annotations.{Key, Reader}

import java.time.Instant
case class Workset(@Reader(idReader) @Key("_id") id: WorksetId, htids: IdSet, created: Instant)
