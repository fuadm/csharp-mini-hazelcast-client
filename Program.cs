using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Collections.Concurrent;
using System.Threading;

namespace Hazelcast.Client
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Program program = new Program();
            program.Start();


        }
        readonly TcpClient tcpClient;
        readonly ConcurrentDictionary<long, Call> calls;
        readonly InThread inThread;
        readonly OutThread outThread;

        private static int THREAD_COUNT = 10;
        static int puts;
        public Program() { 
            this.tcpClient = new TcpClient("localhost", 5701);
            calls = new ConcurrentDictionary<long, Call>();

            inThread = new InThread(tcpClient, calls);
            outThread = new OutThread(tcpClient, calls);

            inThread.start("hz.client.");
            outThread.start("hz.client.");
        }

        public void Start()
        {
            for (int i = 0; i < THREAD_COUNT; i++)
            {
                Thread thread = new Thread(new ThreadStart(this.DoPut));
                thread.Start();
                thread.Name = "HZ_CLIENT_Thread_" + i;
                Console.WriteLine("Thread" + i + " started");
            }
            Thread statThread = new Thread(new ThreadStart(this.Stats));

            statThread.Start();
        }
        public void Stats()
        {
            while (true)
            {
                Thread.Sleep(10000);
                Console.WriteLine("PUTS per second: " + Interlocked.Exchange(ref puts, 0) / 10);
            }

        }

        public void DoPut() {
           
            for (int c=0; ; c++)
            {
                Packet request = new Packet();
                Random random = new Random(5);
               
                byte[] key = new byte[100];
                byte[] val = new byte[1000];
                random.NextBytes(key);
                request.set("c:default", ClusterOperation.CONCURRENT_MAP_PUT, key, val);
                Call call = new Call(request);
                outThread.enQueue(call);
                Packet result = null;
                int timeout = 5000;
                for (int i = 1; ; i++)
                {
                    result = call.getResult(timeout);
                    if (result != null)
                    {
                        if (result.callId != request.callId)
                        {
                            Console.WriteLine("Id's don't match!");
                        }
                        Interlocked.Increment(ref puts);
                        break;
                    }
                    else {
                        Console.WriteLine("Call"  + call.getId() + " didn't get answer within "+ timeout*i/1000 + " seconds");
                    }
                }
                if(c%1000==0){
                    Console.WriteLine("Thread" + Thread.CurrentThread.Name + ":" + c);  
                }
            }
        }
    }
}
