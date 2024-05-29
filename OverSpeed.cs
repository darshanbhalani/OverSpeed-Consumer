using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace Kafka_Consumer
{
    internal class OverSpeed
    {
        private List<DataModel> dataList = new List<DataModel>();
        private List<List<IncidentModel>> allIncidents = new List<List<IncidentModel>>();
        internal DateTime lastExecutionTime = DateTime.MinValue;
        int thresholdTime = 10;
        int thresholdSpeed = 55;
        internal void dataConsumer(ConsumerConfig _config,IConfiguration _configuration)
        {
            

            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                consumer.Subscribe(_configuration["BootstrapService:Topic"]);
                
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };
                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            var msg = JsonConvert.DeserializeObject<List<DataModel>>(cr.Value.ToString());
                            dataList.AddRange(msg!);
                            incidentChecker();
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }

        private void incidentChecker()
        {

            if ((DateTime.Now - lastExecutionTime).TotalSeconds >= thresholdTime)
            {
                

                DateTime startTime = dataList.Min(d => DateTime.Parse(d.TimeStamp.ToString()));
                DateTime endTime = dataList.Max(d => DateTime.Parse(d.TimeStamp.ToString()));

                var groupedData = dataList.GroupBy(d => d.VehicleNumber);

                Console.Clear();
                Console.WriteLine($"\nThreshold Time = {thresholdTime} seconds");
                Console.WriteLine($"Threshold Speed = {thresholdSpeed} Km/h");
                double averageSpeed = dataList.Average(d => d.VehicleSpeed);
                Console.WriteLine($"Total Vehicles = {groupedData.Count()}\n");

                List<IncidentModel> incidents = new List<IncidentModel>();
                foreach (var group in groupedData)
                {
                    IncidentModel incident = new IncidentModel
                    {
                        VehicleNumber = group.Key,
                        AverageSpeed = group.Average(d => d.VehicleSpeed),
                        StartTime = startTime,
                        EndTime = endTime
                    };
                    incidents.Add(incident);
                }

                allIncidents.Add(incidents);

                foreach (var batchIncidents in allIncidents)
                {
                    Console.ForegroundColor = ConsoleColor.Blue;
                    Console.WriteLine("-----------------------------------------------------------------------------------");
                    Console.WriteLine($"| {"Vehicle Number",-15} | {"Average Speed",-15} | {"Start Time",-20} | {"End Time",-20} |");
                    Console.WriteLine("-----------------------------------------------------------------------------------");
                    Console.ResetColor();

                    foreach (var incident in batchIncidents)
                    {
                        if (incident.AverageSpeed > thresholdSpeed)
                        {
                            Console.ForegroundColor = ConsoleColor.Black;
                            Console.BackgroundColor = ConsoleColor.Red;
                        }
                        else if (thresholdSpeed - incident.AverageSpeed <= 5)
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                        }

                        Console.WriteLine($"| {incident.VehicleNumber,-15} | {incident.AverageSpeed.ToString("F2"),-15} | {incident.StartTime,-20} | {incident.EndTime,-20} |");
                        Console.ResetColor();
                    }
                    Console.ForegroundColor = ConsoleColor.Blue;
                    Console.WriteLine("-----------------------------------------------------------------------------------");
                    Console.ResetColor();
                    Console.Write("Loading...");
                }
                
                dataList.Clear();
                allIncidents.Clear();
                lastExecutionTime = DateTime.Now;
            }
        }
    }
}
