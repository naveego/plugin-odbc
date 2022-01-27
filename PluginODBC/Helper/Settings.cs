using System;

namespace PluginODBC.Helper
{
    public class Settings
    {
        public string ConnectionString { get; set; }
        public string Password { get; set; }
        public int QueryTimeout { get; set; } = 30;
        public string PrePublishQuery { get; set; }
        public string PostPublishQuery { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(ConnectionString))
            {
                throw new Exception("the ConnectionString property must be set");
            }
        }

        public string GetConnectionString()
        {
            return ConnectionString.Replace("PASSWORD", Password);
        }
    }
}