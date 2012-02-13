using System;
using System.Threading;
using System.Net.Sockets;
using System.Collections;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;

namespace Hazelcast.Client
{
	public class OutThread:ClientThread 	
	{
        TcpClient tcpClient;
		readonly ConcurrentDictionary<long, Call> calls;
        bool headersWritten = false;
		
		readonly BlockingCollection<Call> inQ = new BlockingCollection<Call>(1000);
        public OutThread(TcpClient tcpClient, ConcurrentDictionary<long, Call> calls)
		{
            this.tcpClient = tcpClient;
			this.calls = calls;
		}

		protected override void customRun ()
		{			
			//long t = System.DateTime.Now.Ticks;
			Call call = inQ.Take();
			//Call call = null;
			//if(!inQ.TryTake(out call)){
			//	return;
			//}
			
			call.pre.Stop();
			call.on.Start();				
			if(!call.FireNforget)
				calls.TryAdd(call.getId (), call);
			Packet packet = call.getRequest ();
			//Console.WriteLine("Writing call " + call.getId());
			if (packet != null) {
				
				write(tcpClient, packet);
				
			}
		}
		
		public bool contains(Call call){
			return inQ.Contains(call);
		}
		
		public static void send (TcpClient tcpClient, Packet packet)
		{
			Stream stream = tcpClient.GetStream();
			packet.write (stream);
		}


		public void enQueue (Call call)
		{
			//long t = System.DateTime.Now.Ticks;
			inQ.Add(call);
			//Console.WriteLine("Add in ticks" + (System.DateTime.Now.Ticks - t));
		}

		public OutThread start (String prefix)
		{
			Thread thread = new Thread (new ThreadStart (this.run));
			this.thread = thread;
			thread.Name = prefix + "OutThread";
			thread.Start ();
			return this;
		}
		
		public void write(TcpClient tcpClient, Packet packet){
	        
	            Stream stream = tcpClient.GetStream();
	            if (!headersWritten) {
	                stream.Write(Packet.HEADER, 0, Packet.HEADER.Length);
	                headersWritten = true;
                    Console.WriteLine("Header is sent");
	            }
                send(tcpClient, packet);    
		}
	}
}

