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
                            //Console.WriteLine("Consumer Started...");

                            var cr = consumer.Consume(cts.Token);
                            var msg = JsonConvert.DeserializeObject<List<DataModel>>(cr.Value.ToString());
                            dataList.AddRange(msg!);
                            incidentChecker();
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                        //Thread.Sleep(1000);
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

            //Console.WriteLine("Incident Checker Started...");

            if ((DateTime.Now - lastExecutionTime).TotalSeconds >= thresholdTime)
            {
                Console.Clear();
                Console.WriteLine($"Threshold Time = {thresholdTime} seconds");
                Console.WriteLine($"Threshold Speed = {thresholdSpeed} Km/h");
                double averageSpeed = dataList.Average(d => d.VehicleSpeed);

                DateTime startTime = dataList.Min(d => DateTime.Parse(d.TimeStamp.ToString()));
                DateTime endTime = dataList.Max(d => DateTime.Parse(d.TimeStamp.ToString()));

                var groupedData = dataList.GroupBy(d => d.VehicleNumber);

                //Console.WriteLine("-----------------------------------------------");
                //foreach (var x in groupedData)
                //{
                //    Console.WriteLine($"{x.Key} --->");
                //    foreach (var value in x)
                //    {
                //        Console.WriteLine($"  Value: {value.VehicleSpeed}");
                //    }
                //    Console.WriteLine();
                //}
                //Console.WriteLine("-----------------------------------------------");

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

                Console.WriteLine("-----------------------------------------------");
                foreach (var batchIncidents in allIncidents)
                {
                    Console.WriteLine("Batch:");
                    foreach (var incident in batchIncidents)
                    {
                        if (incident.AverageSpeed > thresholdSpeed)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                        }
                        else if(thresholdSpeed - incident.AverageSpeed <= 5  )
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                        }
                        Console.WriteLine($"Vehicle Number: {incident.VehicleNumber}, Average Speed: {incident.AverageSpeed}, Start Time: {incident.StartTime}, End Time: {incident.EndTime}");
                        Console.ResetColor();
                        
                    }
                }
                Console.WriteLine("-----------------------------------------------");
                dataList.Clear();
                allIncidents.Clear();
                //Console.WriteLine("Data Clean");
                lastExecutionTime = DateTime.Now;
            }
        }
    }
}
