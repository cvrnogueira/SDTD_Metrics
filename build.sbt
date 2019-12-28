lazy val root = (project in file("."))
  .settings(Compilation.buildSettings: _*)
  .settings(Compilation.settings: _*)