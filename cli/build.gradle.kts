plugins {
    `java-library`
    application
}

dependencies {
    implementation(project(":core"))
    // JLine 3: terminal handling, history, completion, highlighting.
    // One dep, zero transitives, ~1 MB. Justified because raw-terminal
    // handling in Java without it is brittle across OSes.
    implementation("org.jline:jline:3.27.1")
}

application {
    mainClass = "io.jpointdb.cli.Main"
    applicationDefaultJvmArgs = listOf(
        "--enable-native-access=ALL-UNNAMED"
    )
}
