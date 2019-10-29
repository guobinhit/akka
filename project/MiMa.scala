/*
 * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import scala.collection.immutable
import sbt._
import sbt.Keys._
import com.typesafe.tools.mima.plugin.MimaPlugin
import com.typesafe.tools.mima.plugin.MimaPlugin.autoImport._

object MiMa extends AutoPlugin {

  private val latestPatchOf25 = 26
  // No 2.6 has been released yet. Update to '0' after releasing 2.6.0
  private val latestPatchOf26 = -1

  override def requires = MimaPlugin
  override def trigger = allRequirements

  override val projectSettings = Seq(
    mimaPreviousArtifacts := akkaPreviousArtifacts(name.value, organization.value, scalaBinaryVersion.value))

  def akkaPreviousArtifacts(
      projectName: String,
      organization: String,
      scalaBinaryVersion: String): Set[sbt.ModuleID] = {

    val versions: Seq[String] = {
      val firstPatchOf25 =
        if (scalaBinaryVersion.startsWith("2.13")) 25
        else if (projectName.contains("discovery")) 19
        else if (projectName.contains("coordination")) 22
        else 0
    
      if (!projectName.contains("typed")) {
        // 2.5.18 is the only release built with Scala 2.12.7, which due to
        // https://github.com/scala/bug/issues/11207 produced many more
        // static methods than expected. These are hard to filter out, so
        // we exclude it here and rely on the checks for 2.5.17 and 2.5.19.
        expandVersions(2, 5, ((firstPatchOf25 to latestPatchOf25).toSet - 18).toList)
      } else {
        Nil
      } ++ expandVersions(2, 6, 0 to latestPatchOf26)
    }

    val akka25PromotedArtifacts = Set("akka-distributed-data")
    val akkaTypedModules = Set("akka-actor-typed")

    // check against all binary compatible artifacts
    versions.map { v =>
      val adjustedProjectName =
        if (akka25PromotedArtifacts(projectName) && v.startsWith("2.4"))
          projectName + "-experimental"
        else
          projectName
      organization %% adjustedProjectName % v
    }.toSet
  }

  private def expandVersions(major: Int, minor: Int, patches: immutable.Seq[Int]): immutable.Seq[String] =
    patches.map(patch => s"$major.$minor.$patch")
}
