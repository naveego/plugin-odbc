using System;
using Xunit;
using PluginODBC.Helper;

namespace PluginODBCTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateTest()
        {
            // setup
            var settings = new Settings
            {
                ConnectionString = "connection string",
                Password = "pass"
            };
            
            // act
            settings.Validate();

            // assert
        }
        
        [Fact]
        public void ValidateNullUsernameTest()
        {
            // setup
            var settings = new Settings
            {
                ConnectionString = null,
                Password = "pass"
            };
            
            // act
            Exception e  = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("the ConnectionString property must be set", e.Message);
        }
    }
}