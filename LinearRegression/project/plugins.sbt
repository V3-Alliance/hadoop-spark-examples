resolvers += Resolver.url("sbt-plugin-releases-scalasbt", url("http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/"))
resolvers += Resolver.sonatypeRepo("public")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")