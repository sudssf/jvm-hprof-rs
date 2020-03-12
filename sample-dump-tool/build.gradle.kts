plugins {
    kotlin("jvm") version "1.3.70"
    application
}

repositories {
    jcenter()
}

dependencies {
    implementation(kotlin("stdlib"))
}

application {
    mainClassName = "org.mpierce.hprof.HprofSampleTool"
}
