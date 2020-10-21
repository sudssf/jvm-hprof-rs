plugins {
    kotlin("jvm") version "1.4.10"
    application
}

repositories {
    jcenter()
}

dependencies {
    implementation("com.github.ajalt.clikt:clikt:3.0.1")
}

application {
    mainClass.set("org.mpierce.hprof.HprofSampleTool")
}
