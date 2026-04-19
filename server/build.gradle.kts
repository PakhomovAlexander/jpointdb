plugins {
    `java-library`
    application
}

dependencies {
    implementation(project(":core"))
}

application {
    mainClass = "io.jpointdb.server.Main"
    applicationDefaultJvmArgs = listOf(
        "--add-modules=jdk.incubator.vector",
        "--enable-native-access=ALL-UNNAMED"
    )
}
