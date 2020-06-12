import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.github.johnrengelman.shadow") version "5.2.0"
    //id("io.quarkus") version "1.5.1"
}

tasks.withType<ShadowJar> {
    mergeServiceFiles()
    archiveBaseName.set("kawfka-uber")
    manifest {
        attributes["Main-Class"] = "com.kawamind.kawfka.Kawfka"
    }
}

tasks {
    build {
        dependsOn(shadowJar)
    }
    test {
        dependencies {
            implementation("org.junit.jupiter:junit-jupiter-engine:5.4.2")
        }
        useJUnitPlatform()
    }
}

group = "com.kawamind"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.avro:avro:1.9.0")
    implementation("io.confluent:kafka-avro-serializer:5.2.1")
    implementation("org.projectlombok:lombok:1.18.12")
    implementation("io.confluent:common-config:5.2.1")
    implementation("io.confluent:kafka-schema-registry-client:5.2.1")
    implementation("io.projectreactor.kafka:reactor-kafka:1.2.1.RELEASE")
    implementation("org.slf4j:slf4j-api:1.7.25")
    implementation("org.slf4j:slf4j-jdk14:1.7.25")
    implementation("log4j:log4j:1.2.17")
    implementation("com.google.code.gson:gson:2.8.6")
    implementation("info.picocli:picocli:4.2.0")
    implementation("com.googlecode.json-simple:json-simple:1.1.1")
    implementation("com.google.guava:guava:28.2-jre")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.4.2")
    compileOnly("org.projectlombok:lombok:1.18.12")
    annotationProcessor("org.projectlombok:lombok:1.18.12")
    testCompileOnly("org.projectlombok:lombok:1.18.12")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.12")
    implementation("info.picocli:picocli:4.2.0")
    annotationProcessor("info.picocli:picocli-codegen:4.2.0")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}



repositories {
    maven {
        mavenLocal()
        url = uri("http://127.0.0.1:8096/repository/maven-public/")
    }
}

