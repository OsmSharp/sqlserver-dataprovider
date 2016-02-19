// OsmSharp - OpenStreetMap (OSM) SDK
//
// Copyright (C) 2013 Abelshausen Ben
// 
// This file is part of OsmSharp.
// 
// OsmSharp is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
// 
// OsmSharp is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with OsmSharp. If not, see <http://www.gnu.org/licenses/>.

using System;

namespace OsmSharp.Db.SQLServer
{
    /// <summary>
    /// Contains some extensions.
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Converts the given value into a proper db value.
        /// </summary>
		public static object ConvertToDBValue<T>(this T? nullable) where T : struct
        {
            return nullable.HasValue ? (object)nullable.Value : DBNull.Value;
        }

        /// <summary>
        /// Ticks since 1/1/1970
        /// </summary>
        public static long EpochTicks = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        /// <summary>
        /// Converts a number of milliseconds from 1/1/1970 into a standard DateTime.
        /// </summary>
        /// <returns></returns>
        public static DateTime FromUnixTime(this long milliseconds)
        {
            return new DateTime(EpochTicks + milliseconds * 10000); // to a multiple of 100 nanosec or ticks.
        }

        /// <summary>
        /// Converts a standard DateTime into the number of milliseconds since 1/1/1970.
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public static long ToUnixTime(this DateTime date)
        {
            return (date.Ticks - EpochTicks) / 10000; // from a multiple of 100 nanosec or ticks to milliseconds.
        }

        /// <summary>
        /// Returns a trucated string if the string is larger than the given max length.
        /// </summary>
        public static string Truncate(this string value, int maxLength)
        {
            if (value != null && value.Length > maxLength)
            {
                return value.Substring(0, maxLength);
            }
            return value;
        }
    }
}
