plugins {
    `java-library`
    application
}

dependencies {
    implementation(project(":core"))
    implementation(project(":server"))
    testImplementation("org.junit.jupiter:junit-jupiter-params")
}

application {
    mainClass = "io.jpointdb.bench.ClickBench"
    applicationDefaultJvmArgs = listOf(
        "--add-modules=jdk.incubator.vector",
        "--enable-native-access=ALL-UNNAMED"
    )
}

tasks.withType<Test>().configureEach {
    systemProperty("jpoint.benchDir", project.projectDir.absolutePath)
}
