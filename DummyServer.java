
import com.hazelcast.client.Packet;
import com.hazelcast.client.PacketReader;
import com.hazelcast.client.PacketWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class DummyServer {

    public static void main(String[] args) throws IOException {
        ServerSocket server = new ServerSocket(5701);
        System.out.println("Server started");
        final Socket socket = server.accept();
        System.out.println("Accepting a connection");
        new Thread(new Runnable(){
            public void run() {
                try {
                    System.out.println("Thread started to handle the connection");
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dis.readByte();
                    dis.readByte();
                    dis.readByte();
                    for(int i=0;i<100000; i++){
                        Packet packet = new Packet();
                        packet.readFrom(new PacketReader(), dis);
                        //packet.read(inBuffer);
                        System.out.println("Read " + packet);
                        byte[] value = packet.getValue();
                        clearForResponse(packet);
                        packet.setValue(value);
                        packet.writeTo(new PacketWriter(), dos);
                        System.out.println("Sent the response " + packet);

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void clearForResponse(Packet packet) {
        packet.setName(null);
        packet.setKey(null);
        packet.setValue(null);
        packet.setTimeout(-1);
        packet.setTtl(-1);
        packet.setThreadId(-1);
        packet.setLongValue(Long.MIN_VALUE);
    }
}

