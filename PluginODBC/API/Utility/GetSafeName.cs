namespace PluginODBC.API.Utility
{
    public static partial class Utility
    {
        public static string GetSafeName(string unsafeName, char escapeChar = '"')
        {
            return $"{escapeChar}{unsafeName}{escapeChar}";
        }
    }
}