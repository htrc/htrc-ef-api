logLevel := Level.Warn

addSbtPlugin("com.typesafe.play"      % "sbt-plugin"          % "2.8.15")
addSbtPlugin("com.github.dwickern"    % "sbt-swagger-play"    % "0.5.0")

addSbtPlugin("com.typesafe.sbt"       % "sbt-git"             % "1.0.2")
addSbtPlugin("com.github.sbt"         % "sbt-native-packager" % "1.9.9")
addSbtPlugin("com.eed3si9n"           % "sbt-assembly"        % "1.2.0")
addSbtPlugin("org.wartremover"        % "sbt-wartremover"     % "2.4.18")
addSbtPlugin("io.crashbox"            % "sbt-gpg"             % "0.2.1")
addSbtPlugin("com.eed3si9n"           % "sbt-buildinfo"       % "0.11.0")
