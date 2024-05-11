logLevel := Level.Warn

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addSbtPlugin("com.typesafe.play"      % "sbt-plugin"          % "2.9.3")
addSbtPlugin("com.github.dwickern"    % "sbt-swagger-play"    % "0.6.0")

addSbtPlugin("com.github.sbt"         % "sbt-git"             % "2.0.1")
addSbtPlugin("com.github.sbt"         % "sbt-native-packager" % "1.10.0")
addSbtPlugin("com.eed3si9n"           % "sbt-assembly"        % "2.2.0")
addSbtPlugin("com.github.sbt"         % "sbt-pgp"             % "2.2.1")
addSbtPlugin("com.github.sbt"         % "sbt-dynver"          % "5.0.1")
addSbtPlugin("com.eed3si9n"           % "sbt-buildinfo"       % "0.12.0")
