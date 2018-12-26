package chatdemo

import org.jgroups.JChannel
import org.jgroups.Message
import org.jgroups.Receiver
import org.jgroups.View
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


class ChatDemo(arrayOfProtocols: Array<Protocol>, name: String) {

    companion object {
        const val HISTORY_SIZE = 10
    }

    var messageHistory = ArrayDeque<String>()

    val channel = JChannel(*arrayOfProtocols).name(name)

    init {
        channel.receiver = object : Receiver {
            override fun getState(output: OutputStream?) {
                val stream = ObjectOutputStream(output)
                stream.writeObject(this@ChatDemo.messageHistory)
            }

            override fun viewAccepted(new_view: View?) {
                println(new_view)
            }

            override fun receive(msg: Message?) {
                msg?.buffer?.let {
                    println("${msg.src}: ${String(msg.buffer)}")
                    messageHistory.push("${msg.src}: ${String(msg.buffer)}")
                    if (messageHistory.size > ChatDemo.HISTORY_SIZE) {
                        messageHistory.pop()
                    }
                }
            }

            override fun setState(input: InputStream?) {
                val stream = ObjectInputStream(input)
                val state = stream.readObject()
                if (state is ArrayDeque<*>) {
                    for (message in state) {
                        if (message is String) {
                            this@ChatDemo.messageHistory.push(message)
                            if (this@ChatDemo.messageHistory.size > ChatDemo.HISTORY_SIZE) {
                                this@ChatDemo.messageHistory.pop()
                            }
                        }
                    }
                }
            }
        }
    }
}

