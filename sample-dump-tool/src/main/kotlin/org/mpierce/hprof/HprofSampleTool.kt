package org.mpierce.hprof

import com.sun.management.HotSpotDiagnosticMXBean
import java.lang.management.ManagementFactory
import java.nio.file.Paths
import java.time.Instant


object HprofSampleTool {
    @JvmStatic
    fun main(args: Array<String>) {
        when (val mode = args[0]) {
            "startup-heap" -> dumpHeap("startup")
        }
    }
}

fun dumpHeap(prefix: String) {
    val server = ManagementFactory.getPlatformMBeanServer()
    val mxBean = ManagementFactory.newPlatformMXBeanProxy(
            server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean::class.java)

    val path= Paths.get(".").resolve("$prefix-${Instant.now()}.hprof")

    mxBean.dumpHeap(path.toString(), true)
    println("Wrote heap to $path")
}

