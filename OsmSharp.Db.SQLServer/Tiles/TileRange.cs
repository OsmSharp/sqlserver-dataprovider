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

using System.Collections.Generic;

namespace OsmSharp.Db.SQLServer.Tiles
{
    /// <summary>
    /// Represents a range of tiles.
    /// </summary>
    class TileRange : IEnumerable<Tile>
    {
        /// <summary>
        /// Creates a new tile range.
        /// </summary>
        public TileRange(int xMin, int yMin, int xMax, int yMax, int zoom)
        {
            this.XMin = xMin;
            this.XMax = xMax;
            this.YMin = yMin;
            this.YMax = yMax;

            this.Zoom = zoom;
        }

        /// <summary>
        /// The minimum X of this range.
        /// </summary>
        public int XMin { get; private set; }

        /// <summary>
        /// The minimum Y of this range.
        /// </summary>
        public int YMin { get; private set; }

        /// <summary>
        /// The maximum X of this range.
        /// </summary>
        public int XMax { get; private set; }

        /// <summary>
        /// The maximum Y of this range.
        /// </summary>
        public int YMax { get; private set; }

        /// <summary>
        /// The zoom of this range.
        /// </summary>
        public int Zoom { get; private set; }

        /// <summary>
        /// Returns the number of tiles in this range.
        /// </summary>
        public int Count
        {
            get
            {
                return System.Math.Abs(this.XMax - this.XMin + 1) *
                    System.Math.Abs(this.YMax - this.YMin + 1);
            }
        }

        /// <summary>
        /// Returns true if the given tile exists in this range.
        /// </summary>
        public bool Contains(Tile tile)
        {
            return this.XMax >= tile.X && this.XMin <= tile.X &&
                this.YMax >= tile.Y && this.YMin <= tile.Y;
        }

        #region Functions

        /// <summary>
        /// Returns true if the given tile lies at the border of this range.
        /// </summary>
        public bool IsBorderAt(int x, int y, int zoom)
        {
            return ((x == this.XMin) || (x == this.XMax)
                || (y == this.YMin) || (y == this.YMin)) &&
                this.Zoom == zoom;
        }

        /// <summary>
        /// Returns true if the given tile lies at the border of this range.
        /// </summary>
        public bool IsBorderAt(Tile tile)
        {
            return IsBorderAt(tile.X, tile.Y, tile.Zoom);
        }

        #endregion

        #region Conversion Functions

        /// <summary>
        /// Returns a tile range that encompasses the given bounding box at a given zoom level.
        /// </summary>
        public static TileRange CreateAroundBoundingBox(float lat1, float lon1, float lat2, float lon2, int zoom)
        {
            var minLon = lon1;
            var maxLon = lon2;
            var minLat = lat1;
            var maxLat = lat2;

            if (lat1 < lat2)
            {
                minLat = lat1;
                maxLat = lat2;
            }
            else
            {
                minLat = lat2;
                maxLat = lat1;
            }

            if (lon1 < lon2)
            {
                minLon = lon1;
                maxLon = lon2;
            }
            else
            {
                minLon = lon2;
                maxLon = lon1;
            }

            var n = (int)System.Math.Floor(System.Math.Pow(2, zoom));

            var rad = maxLat.ToRadians();

            var xTileMin = (int)(((minLon + 180.0f) / 360.0f) * (double)n);
            var yTileMin = (int)((1.0f - (System.Math.Log(System.Math.Tan(rad) + (1.0f / System.Math.Cos(rad))))
                / System.Math.PI) / 2f * (double)n);

            rad = minLat.ToRadians();
            var xTileMax = (int)(((maxLon + 180.0f) / 360.0f) * (double)n);
            var yTileMax = (int)((1.0f - (System.Math.Log(System.Math.Tan(rad) + (1.0f / System.Math.Cos(rad))))
                / System.Math.PI) / 2f * (double)n);

            return new TileRange(xTileMin, yTileMin, xTileMax, yTileMax, zoom);
        }

        #endregion

        /// <summary>
        /// Returns en enumerator of tiles.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<Tile> GetEnumerator()
        {
            return new TileRangeEnumerator(this);
        }

        /// <summary>
        /// Returns en enumerator of tiles.
        /// </summary>
        /// <returns></returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// Simple enumerator.
        /// </summary>
        private class TileRangeEnumerator : IEnumerator<Tile>
        {
            private TileRange _range;

            private Tile? _current;

            public TileRangeEnumerator(TileRange range)
            {
                _range = range;
            }

            public Tile Current
            {
                get
                {
                    return _current.Value;
                }
            }

            public void Dispose()
            {
                _range = null;
            }

            object System.Collections.IEnumerator.Current
            {
                get { return this.Current; }
            }

            public bool MoveNext()
            {
                if (_current == null)
                {
                    _current = new Tile(_range.XMin, _range.YMin, _range.Zoom);
                    return true;
                }

                var x = _current.Value.X;
                var y = _current.Value.Y;

                if (x == _range.XMax)
                {
                    if (y == _range.YMax)
                    {
                        return false;
                    }
                    y++;
                    x = _range.XMin;
                }
                else
                {
                    x++;
                }
                _current = new Tile(x, y, _current.Value.Zoom);
                return true;
            }

            public void Reset()
            {
                _current = null;
            }
        }
    }
}
