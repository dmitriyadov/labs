package chatrpc

import org.jgroups.JChannel
import org.jgroups.Message
import org.jgroups.Receiver
import org.jgroups.View
import org.jgroups.blocks.MethodCall
import org.jgroups.blocks.RequestOptions
import org.jgroups.blocks.ResponseMode
import org.jgroups.blocks.RpcDispatcher
import org.jgroups.protocols.*
import org.jgroups.protocols.pbcast.GMS
import org.jgroups.protocols.pbcast.NAKACK2
import org.jgroups.protocols.pbcast.STABLE
import org.jgroups.stack.Protocol
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.net.InetAddress
import java.util.*


class ChatRPC(arrayOfProtocols: Array<Protocol>, name: String) {

    companion object {
        const val HISTORY_SIZE = 10
    }

    var messageHistory = ArrayDeque<String>()

    val channel = JChannel(*arrayOfProtocols).name(name)

    val rpcDisp = RpcDispatcher(channel, this)

    fun onMessage(message: String, sender: String) {
        println("$sender: $message")
    }

    init {
        channel.receiver = object : Receiver {
            override fun getState(output: OutputStream?) {
                val stream = ObjectOutputStream(output)
                stream.writeObject(this@ChatRPC.messageHistory)
            }

            override fun viewAccepted(new_view: View?) {
                println(new_view)
            }

            override fun receive(msg: Message?) {
            }

            override fun setState(input: InputStream?) {
                val stream = ObjectInputStream(input)
                val state = stream.readObject()
                if (state is ArrayDeque<*>) {
                    for (message in state) {
                        if (message is String) {
                            this@ChatRPC.messageHistory.push(message)
                            if (this@ChatRPC.messageHistory.size > ChatRPC.HISTORY_SIZE) {
                                this@ChatRPC.messageHistory.pop()
                            }
                        }
                    }
                }
            }
        }

    }
}

