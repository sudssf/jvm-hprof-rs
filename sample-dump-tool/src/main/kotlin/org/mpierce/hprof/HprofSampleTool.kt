package org.mpierce.hprof

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.sun.management.HotSpotDiagnosticMXBean
import java.lang.management.ManagementFactory
import java.nio.file.Paths
import java.time.Instant


object HprofSampleTool {
    @JvmStatic
    fun main(args: Array<String>) {
        object : CliktCommand() {
            init {
                subcommands(
                    StartupHeap(),
                    PrimitiveArrays(),
                    Superclasses()
                )
            }

            override fun run() {}
        }.main(args)
    }
}

class StartupHeap : CliktCommand() {
    override fun run() = dumpHeap("startup")
}

class PrimitiveArrays : CliktCommand() {
    override fun run() {
        val arrays = listOf(
            booleanArrayOf(false, true),
            CharArray(10, Int::toChar),
            FloatArray(10) { it.toFloat() * 1.1F },
            DoubleArray(10) { it.toDouble() * 2.2 },
            ByteArray(10, Int::toByte),
            ShortArray(10) { (it.toShort() + 100.toShort()).toShort() },
            IntArray(10) { it + 200 },
            LongArray(10) { it.toLong() + 300L }
        )

        dumpHeap("primitive-arrays")

        // keep arrays alive
        println("dumped ${arrays.size} arrays")
    }
}

class Superclasses : CliktCommand() {
    override fun run() {
        val objects = (0 until 1000).map {
            SpecializedWidget(
                TargetObj(it),
                it,
                (0 until 10).map(::TargetObj),
                it.toLong(),
                (0 until 100).map(::TargetObj).toSet()
            )
        }

        dumpHeap("superclasses")

        println("dumped ${objects.size} top level objects")
    }
}

fun dumpHeap(prefix: String) {
    val server = ManagementFactory.getPlatformMBeanServer()
    val mxBean = ManagementFactory.newPlatformMXBeanProxy(
        server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean::class.java
    )

    val path = Paths.get(".").resolve("$prefix-${Instant.now()}.hprof")

    mxBean.dumpHeap(path.toString(), true)
    println("Wrote heap to $path")
}

/**
 * Exercise superclass navigation
 */
open class BaseWidget(
    val base1: TargetObj,
    val base2: Int
)

@Suppress("unused")
open class Widget(base1: TargetObj,
                  base2: Int,
                  val widget1: List<TargetObj>,
                  val widget2: Long) : BaseWidget(base1, base2)

@Suppress("unused")
class SpecializedWidget(base1: TargetObj,
                        base2: Int,
                        widget1: List<TargetObj>,
                        widget2: Long,
                        val flavors: Set<TargetObj>) : Widget(base1, base2, widget1, widget2)

data class TargetObj(val num: Int)
