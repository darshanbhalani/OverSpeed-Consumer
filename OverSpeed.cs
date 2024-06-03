using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;

namespace Kafka_Consumer
{
    internal class OverSpeed
    {
        private List<DataModel> dataList = new List<DataModel>();
        private List<List<IncidentModel>> allIncidents = new List<List<IncidentModel>>();
        private List<IncidentModel> overspeedIncidents = new List<IncidentModel>();
        internal DateTime lastExecutionTime = DateTime.MinValue;
        int thresholdTime = 10;
        int thresholdSpeed = 55;
        private NpgsqlConnection connection;

        internal async Task dataConsumer(ConsumerConfig _config, IConfiguration _configuration, NpgsqlConnection _connection)
        {
            connection = _connection;
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
                            await incidentChecker();
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

        private async Task incidentChecker()
        {
            if ((DateTime.Now - lastExecutionTime).TotalSeconds >= thresholdTime)
            {


                var groupedData = dataList.GroupBy(d => d.VehicleNumber);

                Console.Clear();
                Console.WriteLine($"\nThreshold Time = {thresholdTime} seconds");
                Console.WriteLine($"Threshold Speed = {thresholdSpeed} Km/h");
                Console.WriteLine($"Total Vehicles = {groupedData.Count()}\n");

                List<IncidentModel> incidents = new List<IncidentModel>();
                foreach (var group in groupedData)
                {
                    IncidentModel incident = new IncidentModel
                    {
                        VehicleNumber = group.Key,
                        AverageSpeed = group.Average(d => d.VehicleSpeed),
                        StartTime = group.Min(d => DateTime.Parse(d.TimeStamp.ToString())),
                        EndTime = group.Max(d => DateTime.Parse(d.TimeStamp.ToString()))
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
                            overspeedIncidents.Add(incident);
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

                    await saveIncidents();
                    Console.ForegroundColor = ConsoleColor.Blue;
                    Console.WriteLine("-----------------------------------------------------------------------------------");
                    Console.ResetColor();
                    Console.Write("Loading...");
                }

                dataList.Clear();
                allIncidents.Clear();
                await checkConfiguration();
                lastExecutionTime = DateTime.Now;
            }
        }

        private async Task saveIncidents()
        {
            if (overspeedIncidents.Count > 0 && connection != null)
            {
                string[] vehiclenumber = overspeedIncidents.Select(model => model.VehicleNumber).ToArray();
                string[] description = overspeedIncidents.Select(model => "").ToArray();
                DateTime[] starttime = overspeedIncidents.Select(model => model.StartTime).ToArray();
                DateTime[] endtime = overspeedIncidents.Select(model => model.EndTime).ToArray();
                overspeedIncidents.Clear();

                using (NpgsqlCommand cmd = new NpgsqlCommand($"select addoverspeedincidents(@in_vehiclenumber,@in_description,@in_thresholdspeed,@in_starttime,@in_endtime);", connection))
                {
                    cmd.Parameters.Add(new NpgsqlParameter("in_vehiclenumber", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = vehiclenumber.ToArray() });
                    cmd.Parameters.Add(new NpgsqlParameter("in_thresholdspeed", thresholdSpeed));
                    cmd.Parameters.Add(new NpgsqlParameter("in_description", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = description.ToArray() });
                    cmd.Parameters.Add(new NpgsqlParameter("in_starttime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = starttime.ToArray() });
                    cmd.Parameters.Add(new NpgsqlParameter("in_endtime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = endtime.ToArray() });
                    cmd.ExecuteNonQuery();
                }
            }
        }

        internal async Task checkConfiguration()
        {
            using (NpgsqlCommand cmd = new NpgsqlCommand($"select * from configurations where configurationid=1 and isdeleted=false;", connection))
            {
                using(var reader=cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        thresholdSpeed = reader.GetInt32(2);
                        thresholdTime = reader.GetInt32(3);
                    }
                }
                
            }
        }
    }
}
