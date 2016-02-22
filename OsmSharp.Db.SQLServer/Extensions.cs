// The MIT License (MIT)

// Copyright (c) 2016 Ben Abelshausen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
