using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OsmSharp.Db.SQLServer.Tiles
{
    static class Tools
    {
        /// <summary>
        /// Converts degrees to radians.
        /// </summary>
        public static double ToRadians(this double degrees)
        {
            return (degrees / 180d) * System.Math.PI;
        }

        /// <summary>
        /// Converts degrees to radians.
        /// </summary>
        public static double ToRadians(this float degrees)
        {
            return (degrees / 180d) * System.Math.PI;
        }

        /// <summary>
        /// Converts radians to degrees.
        /// </summary>
        public static double ToDegrees(this double radians)
        {
            return (radians / System.Math.PI) * 180d;
        }

        /// <summary>
        /// Converts radians to degrees.
        /// </summary>
        public static double ToDegrees(this float radians)
        {
            return (radians / System.Math.PI) * 180d;
        }

        /// <summary>
        /// Normalizes this angle to the range of [0-360[.
        /// </summary>
        public static double NormalizeDegrees(this double degrees)
        {
            if (degrees >= 360)
            {
                var count = System.Math.Floor(degrees / 360.0);
                degrees = degrees - (360.0 * count);
            }
            else if (degrees < 0)
            {
                var count = System.Math.Floor(-degrees / 360.0) + 1;
                degrees = degrees + (360.0 * count);
            }
            return degrees;
        }
    }
}