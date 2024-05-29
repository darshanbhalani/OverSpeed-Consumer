namespace Kafka_Consumer
{
    internal class IncidentModel
    {
        public string VehicleNumber { get; set; }
        public double AverageSpeed { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
    }
}
