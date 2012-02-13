using System;
using System.Threading;
using System.Net.Sockets;
using System.Collections.Concurrent;
using System.IO;


namespace Hazelcast.Client
{
	public class InThread : ClientThread
	{
		
		private ConcurrentDictionary<long, Call> calls;
        TcpClient tcpClient;
		public Int64 lastReceived;
        bool headerRead = false;
			
		
		
		public InThread (TcpClient tcpClient, ConcurrentDictionary<long, Call> calls)
		{
            this.tcpClient = tcpClient;
			this.calls = calls;
		}
		
		protected override void customRun(){

			Packet packet = readPacket(tcpClient);
			if(packet == null)
				return;
			Interlocked.Exchange(ref lastReceived, DateTime.Now.Ticks);
			
			Call call;
                if (calls.TryGetValue(packet.callId, out call))
                {
                    call.on.Stop();
                    call.post.Start();
                    //Console.WriteLine("Received Answer for " + call.getId());
                    call.setResult(packet);
                }
                else
                {
                 Console.WriteLine("Unkown call result: " + packet.callId + ", " + packet.operation);
                }
		}
		
		public static bool equals(byte[] b1, byte[] b2){
			if(b1.Length!=b2.Length){
				return false;
			}
			for(int i=0;i<b1.Length;i++){
				if(b1[i]!=b2[i]){
					return false;
				}
			}
			
			return true;
			
		}
		
		public Packet readPacket(TcpClient tcp){
        	Stream stream = tcp.GetStream();
			if(!headerRead)
			{
				byte[] header = new byte[3];
				stream.Read(header, 0, 3);
				if(equals(header, Packet.HEADER)){
                    Console.WriteLine("Header is equal!");	
				}
				headerRead = true;
			}
			Packet packet = new Packet();
			packet.read(stream);	
			return packet;
    	}
		
		
		
		
		public InThread start(String prefix)
		{
			Thread thread =  new Thread(new ThreadStart(this.run));
			this.thread = thread;
			thread.Name = prefix + "InThread";
			thread.Start();
			return this;
		}
	}
}
