using System;
using System.IO;
using System.Linq;
using Naveego.Sdk.Plugins;
using PluginODBC.Helper;
using Xunit;

namespace PluginODBCTest.Helper
{
    public class LoggerTest
    {
        private static string _logDirectory = "logs";

        [Fact]
        public void VerboseTest()
        {
            var files = Directory.GetFiles(_logDirectory);
            
            // setup
            try
            {
                foreach (var file in files)
                {
                    File.Delete(file);
                }
            }
            catch
            {
            }

            Logger.Init(_logDirectory);
            Logger.SetLogLevel(LogLevel.Trace);

            // act
            Logger.Verbose("verbose");
            Logger.Debug("debug");
            Logger.Info("info");
            Logger.Error(new Exception("error"), "error");
            Logger.CloseAndFlush();

            // assert
            files = Directory.GetFiles(_logDirectory);
            Assert.Single(files);
            
            string[] lines = File.ReadAllLines(files.First());

            Assert.Equal(5, lines.Length);

            // cleanup
            File.Delete(files.First());
        }

        [Fact]
        public void DebugTest()
        {
            var files = Directory.GetFiles(_logDirectory);
            
            // setup
            try
            {
                foreach (var file in files)
                {
                    File.Delete(file);
                }
            }
            catch
            {
            }

            Logger.Init(_logDirectory);
            Logger.SetLogLevel(LogLevel.Debug);

            // act
            Logger.Verbose("verbose");
            Logger.Debug("debug");
            Logger.Info("info");
            Logger.Error(new Exception("error"), "error");
            Logger.CloseAndFlush();

            // assert
            files = Directory.GetFiles(_logDirectory);
            Assert.Single(files);
            
            string[] lines = File.ReadAllLines(files.First());

            Assert.Equal(4, lines.Length);

            // cleanup
            File.Delete(files.First());
        }

        [Fact]
        public void InfoTest()
        {
            var files = Directory.GetFiles(_logDirectory);
            
            // setup
            try
            {
                foreach (var file in files)
                {
                    File.Delete(file);
                }
            }
            catch
            {
            }

            Logger.Init(_logDirectory);
            Logger.SetLogLevel(LogLevel.Info);

            // act
            Logger.Verbose("verbose");
            Logger.Debug("debug");
            Logger.Info("info");
            Logger.Error(new Exception("error"), "error");
            Logger.CloseAndFlush();

            // assert
            files = Directory.GetFiles(_logDirectory);
            Assert.Single(files);
            
            string[] lines = File.ReadAllLines(files.First());

            Assert.Equal(3, lines.Length);

            // cleanup
            File.Delete(files.First());
        }

        [Fact]
        public void ErrorTest()
        {
            var files = Directory.GetFiles(_logDirectory);
            
            // setup
            try
            {
                foreach (var file in files)
                {
                    File.Delete(file);
                }
            }
            catch
            {
            }

            Logger.Init(_logDirectory);
            Logger.SetLogLevel(LogLevel.Error);

            // act
            Logger.Verbose("verbose");
            Logger.Debug("debug");
            Logger.Info("info");
            Logger.Error(new Exception("error"), "error");
            Logger.CloseAndFlush();

            // assert
            files = Directory.GetFiles(_logDirectory);
            Assert.Single(files);
            
            string[] lines = File.ReadAllLines(files.First());

            Assert.Equal(2, lines.Length);

            // cleanup
            File.Delete(files.First());
        }
        
        [Fact]
        public void ConfigureTest()
        {
            var files = Directory.GetFiles(_logDirectory);
            var newLogsPath = "newlogs";
            var newFiles = Directory.GetFiles(newLogsPath);
            
            // setup
            try
            {
                foreach (var file in files)
                {
                    File.Delete(file);
                }
                
                foreach (var file in newFiles)
                {
                    File.Delete(file);
                }
            }
            catch
            {
            }

            Logger.Init(_logDirectory);
            Logger.SetLogLevel(LogLevel.Error);

            // act
            Logger.Verbose("verbose");
            Logger.Debug("debug");
            Logger.Info("info");
            Logger.Error(new Exception("error"), "error");


            Logger.Init(newLogsPath);
            Logger.Verbose("verbose");
            Logger.Debug("debug");
            Logger.Info("info");
            Logger.Error(new Exception("error"), "error");
            Logger.CloseAndFlush();

            // assert
            files = Directory.GetFiles(_logDirectory);
            Assert.Single(files);
            
            newFiles = Directory.GetFiles(newLogsPath);
            Assert.Single(newFiles);
            
            string[] lines = File.ReadAllLines(newFiles.First());

            Assert.Equal(2, lines.Length);

            // cleanup
            File.Delete(files.First());
            File.Delete(newFiles.First());
        }
    }
}