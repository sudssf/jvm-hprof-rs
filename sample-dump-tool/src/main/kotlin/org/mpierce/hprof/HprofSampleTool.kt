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
            "primitive-arrays" -> {

                val arrays = listOf(
                        booleanArrayOf(false, true),
                        CharArray(10, Int::toChar),
                        FloatArray(10) { it.toFloat() * 1.1F },
                        DoubleArray(10) { it.toDouble() * 2.2 },
                        ByteArray(10, Int::toByte),
                        ShortArray(10) { (it.toShort() + 100.toShort()).toShort()},
                        IntArray(10) { it + 200 },
                        LongArray(10) { it.toLong() + 300L }
                )

                dumpHeap("primitive-arrays")

                // keep arrays alive
                println("${arrays.size}")

            }
        }
    }
}

fun dumpHeap(prefix: String) {
    val server = ManagementFactory.getPlatformMBeanServer()
    val mxBean = ManagementFactory.newPlatformMXBeanProxy(
            server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean::class.java)

    val path = Paths.get(".").resolve("$prefix-${Instant.now()}.hprof")

    mxBean.dumpHeap(path.toString(), true)
    println("Wrote heap to $path")
}

