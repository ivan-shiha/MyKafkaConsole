namespace KafkaProducer
{
    using Microsoft.Extensions.Configuration;
    using System.IO;

    public class Program
    {
        public static void Main()
        {
            var appSettings = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .Build();

            new Producer(appSettings)
                .StartAsync()
                .GetAwaiter()
                .GetResult();
        }
    }
}
