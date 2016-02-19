using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OsmSharp.Db.SQLServer.Tiles
{
    struct Coordinate
    {
        public Coordinate(float latitude, float longitude)
        {
            this.Latitude = latitude;
            this.Longitude = longitude;
        }

        public float Latitude { get; set; }

        public float Longitude { get; set; }
    }
}
