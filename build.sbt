lazy val root = (project in file(".")).
  settings(
    name := "FROHMD",
    version := "1.0"
  )

libraryDependencies += "com.google.guava" % "guava" % "11.0"

/** Gets sources for sbteclipse **/
EclipseKeys.withSource := true
EclipseKeys.eclipseOutput := Some("./bin/")
EclipseKeys.projectFlavor := EclipseProjectFlavor.Java