showCurrentGitBranch

git.useGitDescribe := true

lazy val commonSettings = Seq(
  organization := "org.hathitrust.htrc",
  organizationName := "HathiTrust Research Center",
  organizationHomepage := Some(url("https://www.hathitrust.org/htrc")),
  scalaVersion := "2.13.6",
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-language:postfixOps",
    "-language:implicitConversions"
  ),
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "HTRC Nexus Repository" at "https://nexus.htrc.illinois.edu/repository/maven-public"
  ),
  externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, mavenCentral = false),
  Compile / packageBin / packageOptions += Package.ManifestAttributes(
    ("Git-Sha", git.gitHeadCommit.value.getOrElse("N/A")),
    ("Git-Branch", git.gitCurrentBranch.value),
    ("Git-Version", git.gitDescribedVersion.value.getOrElse("N/A")),
    ("Git-Dirty", git.gitUncommittedChanges.value.toString),
    ("Build-Date", new java.util.Date().toString)
  ),
  Compile / compile / wartremoverWarnings ++= Warts.unsafe.diff(Seq(
    Wart.DefaultArguments,
    Wart.NonUnitStatements,
    Wart.Any,
    Wart.StringPlusAny
  ))
)

lazy val buildInfoSettings = Seq(
  buildInfoOptions ++= Seq(BuildInfoOption.BuildTime),
  buildInfoPackage := "utils",
  buildInfoKeys ++= Seq[BuildInfoKey](
    "gitSha" -> git.gitHeadCommit.value.getOrElse("N/A"),
    "gitBranch" -> git.gitCurrentBranch.value,
    "gitVersion" -> git.gitDescribedVersion.value.getOrElse("N/A"),
    "gitDirty" -> git.gitUncommittedChanges.value,
    "nameWithVersion" -> s"${name.value} ${version.value}"
  )
)

lazy val ammoniteSettings = Seq(
  libraryDependencies +=
    {
      val version = scalaBinaryVersion.value match {
        case "2.10" => "1.0.3"
        case _ ⇒  "2.4.1"
      }
      "com.lihaoyi" % "ammonite" % version % Test cross CrossVersion.full
    },
  Test / sourceGenerators += Def.task {
    val file = (Test / sourceManaged).value / "amm.scala"
    IO.write(file, """object amm extends App { ammonite.Main.main(args) }""")
    Seq(file)
  }.taskValue,
  connectInput := true,
  outputStrategy := Some(StdoutOutput)
)

lazy val dockerSettings = Seq(
  Docker / maintainer := "Boris Capitanu <capitanu@illinois.edu>",
  dockerBaseImage := "eclipse-temurin:8-jre",
  dockerExposedPorts := Seq(9000),
  dockerRepository := Some("docker.htrc.illinois.edu"),
  dockerUpdateLatest := true,
  Universal / javaOptions ++= Seq(
    // don't write any pid files
    "-Dpidfile.path=/dev/null",
    // reference a logback config file that has no file appenders
    "-Dlogback.configurationFile=conf/logback-prod.xml"
  )
)

lazy val `ef-api` = (project in file("."))
  .enablePlugins(PlayScala, BuildInfoPlugin, GitVersioning, GitBranchPrompt, JavaAppPackaging, DockerPlugin)
  .settings(commonSettings)
  .settings(buildInfoSettings)
  .settings(ammoniteSettings)
  .settings(dockerSettings)
  .settings(
    name := "HTRC-ExtractedFeatures-API",
    libraryDependencies ++= Seq(
      guice,
      filters,
      "com.typesafe.play"             %% "play-streams"                     % "2.8.8",
      "org.reactivemongo"             %% "play2-reactivemongo"              % "1.0.7-play28",
      "org.reactivemongo"             %% "reactivemongo-akkastream"         % "1.0.7",
      "org.scalatestplus.play"        %% "scalatestplus-play"               % "5.1.0"   % Test
    ),
    routesGenerator := InjectedRoutesGenerator
  )


