﻿using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;

namespace Kafka_Consumer
{
    internal class OverSpeed
    {
        private List<DataModel> dataList = new List<DataModel>();
        private List<IncidentModel> overspeedIncidents = new List<IncidentModel>();
        internal DateTime lastExecutionTime = DateTime.MinValue;
        int thresholdTime = 10;
        int thresholdSpeed = 55;
        private NpgsqlConnection connection;

        internal async Task start(NpgsqlConnection _connection, ConsumerConfig _config, IConfiguration _configuration)
        {
            Console.WriteLine("Overspeed Consumer Started...");
            connection = _connection;
            Console.WriteLine("Configuration Checking...");
            checkConfiguration();
            Console.WriteLine("Configuration Fetched Successfully...");
            Console.WriteLine("Data Consuming Started...");
            await dataConsumer(_config, _configuration);
        }

        private async Task dataConsumer(ConsumerConfig _config, IConfiguration _configuration)
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
                    if (incident.AverageSpeed > thresholdSpeed)
                    {
                        incident.ThresholdSpeed = thresholdSpeed;
                        incident.Interval = thresholdTime;
                        overspeedIncidents.Add(incident);
                    }
                }
                dataList.Clear();
                await displayData(incidents);
                await saveIncidents();
                await checkConfiguration();
                lastExecutionTime = DateTime.Now;
            }
        }

        private async Task saveIncidents()
        {
            if (overspeedIncidents.Count > 0 && connection != null)
            {
                try
                {
                    string[] vehiclenumber = overspeedIncidents.Select(model => model.VehicleNumber).ToArray();
                    double[] avgSpeed = overspeedIncidents.Select(model => model.AverageSpeed).ToArray();
                    DateTime[] starttime = overspeedIncidents.Select(model => model.StartTime).ToArray();
                    DateTime[] endtime = overspeedIncidents.Select(model => model.EndTime).ToArray();
                    overspeedIncidents.Clear();

                    using (NpgsqlCommand cmd = new NpgsqlCommand($"select addoverspeedincidents(@in_vehiclenumber,@in_thresholdSpeed,@in_avgspeed,@in_interval,@in_starttime,@in_endtime);", connection))
                    {
                        cmd.Parameters.Add(new NpgsqlParameter("in_vehiclenumber", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = vehiclenumber.ToArray() });
                        cmd.Parameters.Add(new NpgsqlParameter("in_thresholdspeed", thresholdSpeed));
                        cmd.Parameters.Add(new NpgsqlParameter("in_avgspeed", NpgsqlDbType.Array | NpgsqlDbType.Double) { Value = avgSpeed.ToArray() });
                        cmd.Parameters.Add(new NpgsqlParameter("in_interval", thresholdTime));
                        cmd.Parameters.Add(new NpgsqlParameter("in_starttime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = starttime.ToArray() });
                        cmd.Parameters.Add(new NpgsqlParameter("in_endtime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = endtime.ToArray() });
                        cmd.ExecuteNonQuery();
                    }
                }catch(NpgsqlException e)
                {
                    await Console.Out.WriteLineAsync("Database Error : "+e.Message);
                }
                catch(Exception ex)
                {
                    await Console.Out.WriteLineAsync("Error : " + ex.Message);
                }
            }
        }

        private async Task checkConfiguration()
        {
            using (NpgsqlCommand cmd = new NpgsqlCommand($"select * from configurations where configurationid=1 and isdeleted=false;", connection))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        thresholdSpeed = reader.GetInt32(2);
                        thresholdTime = reader.GetInt32(3);
                    }
                }

            }
        }

        private async Task displayData(List<IncidentModel> incidents)
        {
            Console.Clear();
            Console.WriteLine($"\nThreshold Time = {thresholdTime} seconds");
            Console.WriteLine($"Threshold Speed = {thresholdSpeed} Km/h");
            Console.WriteLine($"Total Vehicles = {incidents.Count}\n");
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("-----------------------------------------------------------------------------------");
            Console.WriteLine($"| {"Vehicle Number",-15} | {"Average Speed",-15} | {"Start Time",-20} | {"End Time",-20} |");
            Console.WriteLine("-----------------------------------------------------------------------------------");
            Console.ResetColor();

            foreach (var incident in incidents)
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
    }
}

