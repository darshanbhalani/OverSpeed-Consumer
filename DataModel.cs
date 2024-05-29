using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka_Consumer
{
    internal class DataModel
    {
        public string VehicleNumber { get; set; }
        public int VehicleSpeed { get; set; }
        public DateTime TimeStamp { get; set; }
    }
}
