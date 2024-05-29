using Confluent.Kafka;
using Kafka_Consumer;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

class Program
{
    public static void Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
        .SetBasePath(AppContext.BaseDirectory)
        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
        .Build();
    
        var config = new ConsumerConfig
        {
            GroupId = configuration["BootstrapService:GroupId"],
            BootstrapServers = $"{configuration["BootstrapService:Server"]}:{configuration["BootstrapService:Port"]}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        OverSpeed overSpeed = new OverSpeed();

        overSpeed.dataConsumer(config, configuration);

    }
}
