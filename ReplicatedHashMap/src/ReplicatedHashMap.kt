package hashmap

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
import kotlin.collections.HashMap


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
                println("-- returning ${this@RHashMap.stocks.size} stocks")
                stream.writeObject(this@RHashMap.stocks)
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
                            synchronized(this@RHashMap.stocks) {
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