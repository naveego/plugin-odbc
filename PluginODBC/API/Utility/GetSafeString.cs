namespace PluginODBC.API.Utility
{
    public static partial class Utility
    {
        public static string GetSafeString(string unsafeString, string escapeChar = "\\", string newValue = "\\\\")
        {
            return unsafeString.Replace(escapeChar, newValue);
        }
    }
}