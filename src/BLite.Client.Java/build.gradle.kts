// BLite.Client.Java — Gradle build
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
    id("com.vanniktech.maven.publish") version "0.30.0"
}

group   = "io.blite"
version = "0.1.3"

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

repositories {
    mavenCentral()
}

val grpcVersion     = "1.68.1"
val protobufVersion = "3.25.5"

dependencies {
    // ── gRPC ────────────────────────────────────────────────────────────────
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    runtimeOnly("io.grpc:grpc-netty-shaded:$grpcVersion")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // javax.annotation for generated stubs

    // ── MessagePack (QueryDescriptor serialization) ──────────────────────────
    api("org.msgpack:msgpack-core:0.9.8")

    // ── Spring Boot autoconfigure (optional) ─────────────────────────────────
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:3.4.1")
    compileOnly("org.springframework.data:spring-data-commons:3.4.1")

    // ── Test ─────────────────────────────────────────────────────────────────
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.assertj:assertj-core:3.27.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
    plugins {
        create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion" }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins { create("grpc") }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    signAllPublications()

    coordinates("io.blite", "blite-client-java", version.toString())

    pom {
        name.set("BLite Client Java")
        description.set("Java/Spring Boot client SDK for BLite Server — provides typed access over gRPC.")
        url.set("https://github.com/EntglDb/BLite.Server")
        licenses {
            license {
                name.set("AGPL-3.0-only")
                url.set("https://www.gnu.org/licenses/agpl-3.0.html")
            }
        }
        developers {
            developer {
                id.set("entgldb")
                name.set("Luca Fabbri")
                organization.set("EntglDb")
            }
        }
        scm {
            connection.set("scm:git:git://github.com/EntglDb/BLite.Server.git")
            developerConnection.set("scm:git:ssh://github.com/EntglDb/BLite.Server.git")
            url.set("https://github.com/EntglDb/BLite.Server/tree/main/src/BLite.Client.Java")
        }
    }
}
