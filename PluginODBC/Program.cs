using System;
using System.Linq;
using Grpc.Core;
using Pub;
using PluginODBC.Helper;

namespace PluginODBC
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // Add final chance exception handler
                AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
                {
                    Logger.Error($"died: {eventArgs.ExceptionObject}");
                };
                
                // clean old logs on start up
                Logger.Clean();
            
                // create new server and start it
                Server server = new Server
                {
                    Services = { Publisher.BindService(new Plugin.Plugin()) },
                    Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
                };
                server.Start();
    
                // write out the connection information for the Hashicorp plugin runner
                var output = String.Format("{0}|{1}|{2}|{3}:{4}|{5}",
                    1, 1, "tcp", "localhost", server.Ports.First().BoundPort, "grpc");
            
                Console.WriteLine(output);
            
                Logger.Info("Started on port " + server.Ports.First().BoundPort);
                
                // wait to exit until given input
                Console.ReadLine();
                
                Logger.Info("Plugin exiting...");

                // shutdown server
                server.ShutdownAsync().Wait();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
            }
        }
    }
}