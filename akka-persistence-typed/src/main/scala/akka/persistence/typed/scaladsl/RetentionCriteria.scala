/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import akka.annotation.DoNotInherit
import akka.persistence.typed.internal.DisabledRetentionCriteria
import akka.persistence.typed.internal.SnapshotRetentionCriteriaImpl

/**
 * Criteria for retention/deletion of snapshots and events.
 */
trait RetentionCriteria {
  def asJava: akka.persistence.typed.javadsl.RetentionCriteria
}

/**
 * Criteria for retention/deletion of snapshots and events.
 */
object RetentionCriteria {

  /**
   * Snapshots are not saved and deleted automatically, events are not deleted.
   */
  val disabled: RetentionCriteria = DisabledRetentionCriteria

  /**
   * Save snapshots automatically every `numberOfEvents`. Snapshots that have sequence number
   * less than sequence number of the saved snapshot minus `keepNSnapshots * numberOfEvents` are
   * automatically deleted.
   *
   * Use [[SnapshotRetentionCriteria.withDeleteEventsOnSnapshot]] to
   * delete old events. Events are not deleted by default.
   */
  def snapshotEvery(numberOfEvents: Int, keepNSnapshots: Int): SnapshotRetentionCriteria =
    SnapshotRetentionCriteriaImpl(numberOfEvents, keepNSnapshots, deleteEventsOnSnapshot = false)

}

@DoNotInherit trait SnapshotRetentionCriteria extends RetentionCriteria {

  /**
   * Delete events after saving snapshot via [[RetentionCriteria.snapshotEvery()]].
   * Events that have sequence number less than the snapshot sequence number minus
   * `keepNSnapshots * numberOfEvents` are deleted.
   */
  def withDeleteEventsOnSnapshot: SnapshotRetentionCriteria
}
