import org.jgroups.JChannel
import org.jgroups.Message
import org.jgroups.Receiver
import org.jgroups.View
import org.jgroups.blocks.MessageDispatcher
import org.jgroups.blocks.MethodCall
import org.jgroups.blocks.RequestOptions
import org.jgroups.blocks.RpcDispatcher
import org.jgroups.protocols.*
import org.jgroups.protocols.pbcast.GMS
import org.jgroups.protocols.pbcast.NAKACK2
import org.jgroups.protocols.pbcast.STABLE
import org.jgroups.protocols.pbcast.STATE_SOCK
import org.jgroups.stack.Protocol
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.net.InetAddress
import java.util.*


class ReplicatedHashMap(arrayOfProtocols: Array<Protocol>, name: String) {
    val stocks = HashMap<String, Double>()

    val channel = JChannel(*arrayOfProtocols).name(name)!!

    val rpcDisp = RpcDispatcher(channel, this)

    fun _setStock(name: String, value: Double) {
        synchronized(stocks) {
            stocks[name] = value
        }
        println("-- set stock $name to $value")
    }

    fun _removeStock(name: String) {
        synchronized(stocks) {
            stocks.remove(name)
        }
        println("-- removed stock $name")
    }

    fun _compareAndSwap(key: String, referenceValue: Double, newValue: Double): Boolean {
        if (stocks[key] == referenceValue) {
            synchronized(stocks) {
                stocks[key] = newValue
            }
            println("-- swapped stock $key with value $referenceValue to $newValue")
            return true
        }
        println("-- stock $key with value ${stocks[key]} doesn't match $referenceValue")
        return false
    }


    init {
        val receiver = object : Receiver {
            override fun getState(output: OutputStream?) {
                val stream = ObjectOutputStream(output)
                println("-- returning ${this@ReplicatedHashMap.stocks.size} stocks")
                stream.writeObject(this@ReplicatedHashMap.stocks)
            }

            override fun receive(msg: Message?) {
                println("received message")
            }

            override fun viewAccepted(new_view: View?) {
                println(new_view)
            }


            override fun setState(input: InputStream?) {
                val stream = ObjectInputStream(input)
                val state = stream.readObject()
                if (state is HashMap<*, *>) {
                    println("-- received ${state.size} stocks")
                    stocks.clear()

                    for ((key, value) in state) {
                        if (key is String && value is Double) {
                            synchronized(this@ReplicatedHashMap.stocks) {
                                stocks[key] = value
                            }
                        }
                    }
                }
            }

        }
        channel.receiver = receiver
        rpcDisp.setStateListener<MessageDispatcher>(receiver)
        rpcDisp.start<MessageDispatcher>()

    }

}

fun main(args: Array<String>) {
    if (args.size != 1 || System.getenv("HOSTNAME") == null) {
        println("Specify cluster name in args and HOSTNAME environment variable")
        return
    }
    val arrayOfProtocols = arrayOf(
            UDP().setValue("bind_addr", InetAddress.getLocalHost()),
            PING(),
            MERGE3().setMinInterval(1000).setMaxInterval(5000),
            FD_SOCK(),
            FD_ALL(),
            VERIFY_SUSPECT(),
            BARRIER(),
            NAKACK2(),
            UNICAST3(),
            STABLE(),
            GMS(),
            UFC(),
            MFC(),
            FRAG2(), STATE_SOCK())
    val hashmap = ReplicatedHashMap(arrayOfProtocols, System.getenv("HOSTNAME"))
    hashmap.channel.connect(args[0])
    hashmap.channel.getState(null, 10000)
    val methodSet = hashmap.javaClass.getMethod("_setStock", String::class.java, Double::class.java)
    val methodRemove = hashmap.javaClass.getMethod("_removeStock", String::class.java)
    val compareAndSwapMethod = hashmap.javaClass.getMethod("_compareAndSwap", String::class.java,
            Double::class.java, Double::class.java)
    for (i in 1..1000000) {
        hashmap.rpcDisp.callRemoteMethods<Any>(null, MethodCall(methodSet, "key", i), RequestOptions.SYNC())
    }
    while (true) {
        println("[1] Show stocks [2] Get quote [3] Set quote [4] Remove quote [5] Compare-and-swap [ctrl-D] Exit")
        val line = readLine() ?: break
        when (line) {
            "1" -> {
                println("Stocks: ")
                synchronized(hashmap.stocks) {
                    hashmap.stocks.forEach { key, value -> println("$key: $value") }
                }
            }
            "2" -> {
                println("enter key")
                val key = readLine() ?: println("error")
                println("$key: ${hashmap.stocks[key]}")
            }
            "3" -> {
                println("enter key")
                val key = readLine() ?: println("error")
                println("enter value (double)")
                val result = readLine()
                val value: Double = result!!.toDouble()
                hashmap.rpcDisp.callRemoteMethods<Any>(null, MethodCall(methodSet, key, value), RequestOptions.SYNC())
            }
            "4" -> {
                println("enter key")
                val key = readLine() ?: println("error")
                hashmap.rpcDisp.callRemoteMethods<Any>(null, MethodCall(methodRemove, key), RequestOptions.SYNC())
            }
            "5" -> {
                println("enter key")
                val key = readLine() ?: println("error")
                println("enter reference value (double)")
                var result = readLine()
                val referenceValue: Double = result!!.toDouble()
                println("enter new value (double)")
                result = readLine()
                val newValue: Double = result!!.toDouble()
                hashmap.rpcDisp.callRemoteMethods<Any>(null, MethodCall(compareAndSwapMethod, key,
                        referenceValue, newValue), RequestOptions.SYNC())
            }
        }
    }

    hashmap.channel.close()
}