import net.ltgt.gradle.errorprone.errorprone

plugins {
    java
    id("com.diffplug.spotless") version "6.25.0" apply false
    id("net.ltgt.errorprone") version "4.1.0" apply false
    id("com.github.spotbugs") version "6.0.22" apply false
    id("info.solidsoft.pitest") version "1.19.0-rc.1" apply false
    id("org.owasp.dependencycheck") version "11.1.1" apply false
}

allprojects {
    group = "io.jpointdb"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "checkstyle")
    apply(plugin = "jacoco")
    apply(plugin = "net.ltgt.errorprone")
    apply(plugin = "com.github.spotbugs")
    apply(plugin = "info.solidsoft.pitest")
    apply(plugin = "org.owasp.dependencycheck")

    extensions.configure<org.owasp.dependencycheck.gradle.extension.DependencyCheckExtension> {
        // Only scan runtime / production dependencies; skip test-only configs.
        scanConfigurations = listOf("runtimeClasspath", "compileClasspath")
        // Fail if any HIGH or CRITICAL CVE is found.
        failBuildOnCVSS = 7.0f
        formats = listOf("HTML", "XML")
        // Don't gate the default check — this runs on demand via
        // ./gradlew dependencyCheckAnalyze (downloads NVD feeds ~800 MB first time).
        suppressionFile = rootProject.file("gradle/dependency-check-suppressions.xml").absolutePath
    }

    // Force newest ASM so SpotBugs / PIT / any other bytecode-analyzing tool
    // understands class-file major 69 (Java 25). SpotBugs 6.x and PIT still ship
    // older ASM transitively.
    configurations.configureEach {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.ow2.asm") {
                useVersion("9.8")
            }
        }
    }

    extensions.configure<com.github.spotbugs.snom.SpotBugsExtension> {
        toolVersion.set("4.9.3")
        effort.set(com.github.spotbugs.snom.Effort.DEFAULT)
        reportLevel.set(com.github.spotbugs.snom.Confidence.HIGH)
        ignoreFailures.set(false)
        excludeFilter.set(rootProject.file("gradle/spotbugs-exclude.xml"))
    }

    tasks.withType<com.github.spotbugs.snom.SpotBugsTask>().configureEach {
        // Exclude tests — they intentionally do "smelly" things (try/catch ignored, etc.).
        onlyIf { name == "spotbugsMain" }
        reports.create("html") { required.set(true) }
        reports.create("xml") { required.set(true) }
    }

    extensions.configure<info.solidsoft.gradle.pitest.PitestPluginExtension> {
        // PIT 1.21+ bundles ASM 9.9 which understands class-file major 69.
        pitestVersion.set("1.21.0")
        junit5PluginVersion.set("1.2.1")
        targetClasses.set(listOf("io.jpointdb.*"))
        threads.set(Runtime.getRuntime().availableProcessors())
        outputFormats.set(listOf("HTML", "XML"))
        timestampedReports.set(false)
        // Inherit the same JVM args our regular test task uses; without them the
        // SmokeTest.vectorApiIsAvailable() fails to resolve jdk.incubator.vector.
        jvmArgs.set(listOf(
            "--add-modules=jdk.incubator.vector",
            "--enable-native-access=ALL-UNNAMED"
        ))
    }

    extensions.configure<CheckstyleExtension> {
        toolVersion = "10.18.1"
        configFile = rootProject.file("gradle/checkstyle.xml")
        configProperties = mapOf("suppressionFile" to rootProject.file("gradle/checkstyle-suppressions.xml").absolutePath)
        maxWarnings = 0
        isIgnoreFailures = false
    }

    tasks.withType<Checkstyle>().configureEach {
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    extensions.configure<JacocoPluginExtension> {
        // 0.8.13+ needed to analyse Java 25 bytecode (class-file major 69).
        toolVersion = "0.8.13"
    }

    tasks.withType<JacocoReport>().configureEach {
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(25)
        }
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.11.3"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
        "errorprone"("com.google.errorprone:error_prone_core:2.36.0")
        "errorprone"("com.uber.nullaway:nullaway:0.12.3")
        // JSpecify provides @NullMarked / @Nullable annotations that NullAway understands.
        "api"("org.jspecify:jspecify:1.0.0")
        // Force newer ASM into PIT's runtime classpath so it can parse Java 25 bytecode.
        "pitest"("org.ow2.asm:asm:9.8")
        "pitest"("org.ow2.asm:asm-commons:9.8")
        "pitest"("org.ow2.asm:asm-tree:9.8")
        "pitest"("org.ow2.asm:asm-util:9.8")
    }

    tasks.withType<JavaCompile>().configureEach {
        options.errorprone {
            disableWarningsInGeneratedCode.set(true)
            allErrorsAsWarnings.set(false)
            allDisabledChecksAsWarnings.set(false)
            error(
                "Finally", "OperatorPrecedence", "EscapedEntity", "MutablePublicArray",
                "EmptyCatch", "StringCaseLocaleUsage", "UnusedMethod", "DefaultCharset",
                "AddressSelection",
                "NullAway"
            )
            disable(
                "MissingSummary", "UnusedVariable", "StringSplitter"
            )
            // NullAway: treat every class under io.jpointdb as annotated (non-null by default)
            // and honour JSpecify @NullMarked / @Nullable.
            option("NullAway:AnnotatedPackages", "io.jpointdb")
            option("NullAway:JSpecifyMode", "true")
            option("NullAway:HandleTestAssertionLibraries", "true")
            option("NullAway:AssertsEnabled", "true")
            // Our Maps holding Object values legitimately have nullable values
            // (QueryResult rows) — NullAway Generics mode can be too strict.
            option("NullAway:AcknowledgeRestrictiveAnnotations", "true")
            // Tests use all sorts of null-poking assertions; exclude them.
            option("NullAway:ExcludedFieldAnnotations",
                "org.junit.jupiter.api.io.TempDir")
        }
    }

    // NullAway is noisy during test compilation (assertion utilities, mock patterns).
    // Keep the gate on main sources only.
    tasks.named<JavaCompile>("compileTestJava") {
        options.errorprone.disable("NullAway")
    }

    extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            target("src/*/java/**/*.java")
            // google-java-format and palantir both call javac internals that changed in
            // Java 25 (Log$DeferredDiagnosticHandler.getDiagnostics return type). Eclipse
            // JDT has its own parser and is the only formatter that still works here.
            eclipse("4.33").configFile(rootProject.file("gradle/eclipse-formatter.xml"))
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
            // Workaround: google-java-format insists on 100-char wrap; leave alone otherwise.
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release = 25
        options.compilerArgs.addAll(listOf("--add-modules", "jdk.incubator.vector"))
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        maxHeapSize = "2g"
        jvmArgs(
            "--add-modules=jdk.incubator.vector",
            "--enable-native-access=ALL-UNNAMED",
            "-XX:+UnlockExperimentalVMOptions",
            "-XX:+UseZGC"
        )
        finalizedBy(tasks.named("jacocoTestReport"))
    }

    // Wire coverage report + verification into the standard `check` lifecycle.
    afterEvaluate {
        tasks.findByName("jacocoTestReport")?.let { tasks.named("check") { dependsOn(it) } }
        tasks.findByName("jacocoTestCoverageVerification")?.let { tasks.named("check") { dependsOn(it) } }
    }
}

// Per-module coverage thresholds: bench is a harness (no meaningful unit coverage),
// server/cli are glue, core is the load-bearing engine.
project(":core").afterEvaluate {
    tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
        dependsOn(tasks.named("test"))
        violationRules {
            rule {
                limit {
                    counter = "INSTRUCTION"
                    // Primitive-inline executor path is covered by bench integration
                    // tests (ClickBench golden suite) which JaCoCo doesn't count.
                    // Unit-test threshold tuned down to stay honest.
                    minimum = "0.68".toBigDecimal()
                }
            }
        }
    }
}
project(":server").afterEvaluate {
    tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
        dependsOn(tasks.named("test"))
        violationRules {
            rule {
                limit {
                    counter = "INSTRUCTION"
                    minimum = "0.50".toBigDecimal()
                }
            }
        }
    }
}
project(":cli").afterEvaluate {
    tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
        dependsOn(tasks.named("test"))
        violationRules {
            rule {
                // Most of the CLI is interactive REPL / table rendering that's exercised by
                // subprocess e2e tests only — those tests don't feed JaCoCo. Keep threshold
                // modest: 30 % on the classes we unit-test (parseMeta, printTable helpers).
                limit {
                    counter = "INSTRUCTION"
                    minimum = "0.30".toBigDecimal()
                }
            }
        }
    }
}
project(":bench").afterEvaluate {
    // No threshold on the bench harness; it mainly shells out to the engine in tests.
    tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
        dependsOn(tasks.named("test"))
    }
}
