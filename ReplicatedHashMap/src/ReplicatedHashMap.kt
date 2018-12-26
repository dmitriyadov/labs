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

}