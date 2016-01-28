// OsmSharp - OpenStreetMap (OSM) SDK
//
// Copyright (C) 2016 Abelshausen Ben
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
using System.Collections.Generic;
using System.Data.Common;

namespace OsmSharp.Data.SQLServer.Osm
{
    /// <summary>
    /// A wrapper around the db data reader to keep extra status info.
    /// </summary>
    public class DbDataReaderWrapper : IDisposable
    {
        private readonly DbDataReader _dbDataReader;
        private Dictionary<string, int> _columnIndexes;

        /// <summary>
        /// Creates a new wrapper.
        /// </summary>
        /// <param name="dbDataReader"></param>        
        public DbDataReaderWrapper(DbDataReader dbDataReader)
        {
            _dbDataReader = dbDataReader;
            _columnIndexes = new Dictionary<string, int>(_dbDataReader.FieldCount);

            for (var i = 0; i< _dbDataReader.FieldCount; i++)
            {
                _columnIndexes.Add(_dbDataReader.GetName(i), i);
            }
        }

        private bool _activeRow = false;

        /// <summary>
        /// Returns true if there is an active row.
        /// </summary>
        public bool HasActiveRow
        {
            get
            {
                return _activeRow;
            }
        }

        /// <summary>
        /// Advances the reader to the next row.
        /// </summary>
        /// <returns></returns>
        public bool Read()
        {
            if(_dbDataReader.Read())
            {
                _activeRow = true;
                return true;
            }
            _activeRow = false;
            return false;
        }

        /// <summary>
        /// Returns true if this reader supports the given field.
        /// </summary>
        public bool HasColumn(string name)
        {
            return _columnIndexes.ContainsKey(name);
        }

        /// <summary>
        /// Gets the oridinal for the given name.
        /// </summary>
        public int GetOrdinal(string name)
        {
            int i = -1;
            if(!_columnIndexes.TryGetValue(name, out i))
            {
                return -1;
            }
            return i;
        }

        /// <summary>
        /// Gets a boolean.
        /// </summary>
        public bool GetBoolean(string name)
        {
            int i = -1;
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                throw new ArgumentOutOfRangeException(name);
            }
            return this.GetBoolean(i);
        }

        /// <summary>
        /// Gets a boolean.
        /// </summary>
        public bool GetBoolean(int ordinal)
        {
            return _dbDataReader.GetBoolean(ordinal);
        }

        /// <summary>
        /// Gets a double.
        /// </summary>
        public double GetDouble(string name)
        {
            int i = -1;
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                throw new ArgumentOutOfRangeException(name);
            }
            return this.GetDouble(i);
        }

        /// <summary>
        /// Gets a double.
        /// </summary>
        public double GetDouble(int ordinal)
        {
            return _dbDataReader.GetDouble(ordinal);
        }

        /// <summary>
        /// Gets a long.
        /// </summary>
        public long GetInt64(string name)
        {
            int i = -1;
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                throw new ArgumentOutOfRangeException(name);
            }
            return this.GetInt64(i);
        }

        /// <summary>
        /// Gets a long.
        /// </summary>
        public long GetInt64(int ordinal)
        {
            return _dbDataReader.GetInt64(ordinal);
        }

        /// <summary>
        /// Gets an int.
        /// </summary>
        public int GetInt32(string name)
        {
            int i = -1;
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                throw new ArgumentOutOfRangeException(name);
            }
            return this.GetInt32(i);
        }

        /// <summary>
        /// Gets a int.
        /// </summary>
        public int GetInt32(int ordinal)
        {
            return _dbDataReader.GetInt32(ordinal);
        }

        /// <summary>
        /// Gets an datetime.
        /// </summary>
        public DateTime GetDateTime(string name)
        {
            int i = -1;
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                throw new ArgumentOutOfRangeException(name);
            }
            return this.GetDateTime(i);
        }

        /// <summary>
        /// Gets a datetime.
        /// </summary>
        public DateTime GetDateTime(int ordinal)
        {
            return _dbDataReader.GetDateTime(ordinal);
        }

        /// <summary>
        /// Gets a string.
        /// </summary>
        public string GetString(string name)
        {
            int i = -1;
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                throw new ArgumentOutOfRangeException(name);
            }
            return this.GetString(i);
        }

        /// <summary>
        /// Gets a string.
        /// </summary>
        public string GetString(int ordinal)
        {
            return _dbDataReader.GetString(ordinal);
        }

        /// <summary>
        /// Disposes of all unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            (_dbDataReader as IDisposable).Dispose();
        }
    }
}
