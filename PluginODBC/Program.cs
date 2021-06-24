using System;
using System.Linq;
using Grpc.Core;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using PluginODBC.Helper;

namespace PluginODBC
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // setup logger
                Logger.Init();

                // Add final chance exception handler
                AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
                {
                    Logger.Error(null, $"died: {eventArgs.ExceptionObject}");
                    Logger.CloseAndFlush();
                };
                
                // create new server and start it
                Server server = new Server
                {
                    Services = {Publisher.BindService(new Plugin.Plugin())},
                    Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
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
                Logger.CloseAndFlush();

                // shutdown server
                server.ShutdownAsync().Wait();
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                Logger.CloseAndFlush();
                throw;
            }
        }
    }
}