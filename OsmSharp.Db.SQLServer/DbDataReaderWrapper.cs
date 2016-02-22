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
using System.Collections.Generic;
using System.Data.Common;

namespace OsmSharp.Db.SQLServer
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
        public DbDataReaderWrapper(DbDataReader dbDataReader)
        {
            _dbDataReader = dbDataReader;
            _columnIndexes = new Dictionary<string, int>(_dbDataReader.FieldCount);

            for (var i = 0; i < _dbDataReader.FieldCount; i++)
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
            if (_dbDataReader.Read())
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
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                return -1;
            }
            return i;
        }

        /// <summary>
        /// Returns true if the data for the given column is null.
        /// </summary>
        public bool IsDBNull(string name)
        {
            int i = -1;
            if (!_columnIndexes.TryGetValue(name, out i))
            {
                throw new ArgumentOutOfRangeException(name);
            }
            return this.IsDBNull(i);
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
        /// Gets an object.
        /// </summary>
        public object GetValue(int ordinal)
        {
            return _dbDataReader.GetValue(ordinal);
        }

        /// <summary>
        /// Disposes of all unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            (_dbDataReader as IDisposable).Dispose();
        }

        /// <summary>
        /// Returns true if the value in the given column is null.
        /// </summary>
        public bool IsDBNull(int ordinal)
        {
            return _dbDataReader.IsDBNull(ordinal);
        }

        /// <summary>
        /// Closes this data reader.
        /// </summary>
        public void Close()
        {
            _dbDataReader.Close();
        }

        /// <summary>
        /// Returns true when this db reader is closed.
        /// </summary>
        public bool IsClosed
        {
            get
            {
                return _dbDataReader.IsClosed;
            }
        }
    }

    /// <summary>
    /// Contains extension methods related to the DbDataReaderWrapper.
    /// </summary>
    public static class DbDataReaderWrapperExtension
    {
        /// <summary>
        /// Executes a data reader and wraps it.
        /// </summary>
        public static DbDataReaderWrapper ExecuteReaderWrapper(this DbCommand command)
        {
            return new DbDataReaderWrapper(command.ExecuteReader());
        }
    }
}