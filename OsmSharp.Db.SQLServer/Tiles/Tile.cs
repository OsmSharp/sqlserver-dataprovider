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
using System.Linq;

namespace OsmSharp.Db.SQLServer.Tiles
{
    /// <summary>
    /// Represents a tile.
    /// </summary>
    struct Tile
    {
        /// <summary>
        /// Holds the id.
        /// </summary>
        private ulong _id;

        /// <summary>
        /// Creates a new tile from a given id.
        /// </summary>
        public Tile(ulong id)
        {
            _id = id;

            var tile = Tile.CalculateTile(id);
            this.X = tile.X;
            this.Y = tile.Y;
            this.Zoom = tile.Zoom;
        }

        /// <summary>
        /// Creates a new tile.
        /// </summary>
        public Tile(int x, int y, int zoom)
        {
            this.X = x;
            this.Y = y;
            this.Zoom = zoom;

            _id = Tile.CalculateTileId(zoom, x, y);
        }

        /// <summary>
        /// The X position of the tile.
        /// </summary>
        public int X { get; private set; }

        /// <summary>
        /// The Y position of the tile.
        /// </summary>
        public int Y { get; private set; }

        /// <summary>
        /// The zoom level for this tile.
        /// </summary>
        public int Zoom { get; private set; }

        /// <summary>
        /// Returns a hashcode for this tile position.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return this.X.GetHashCode() ^
                   this.Y.GetHashCode() ^
                   this.Zoom.GetHashCode();
        }

        /// <summary>
        /// Returns true if the given object represents the same tile.
        /// </summary>
        public override bool Equals(object obj)
        {
            if (obj is Tile)
            {
                var other = (Tile)obj;
                return other.X == this.X &&
                        other.Y == this.Y &&
                        other.Zoom == this.Zoom;
            }
            return false;
        }

        /// <summary>
        /// Returns a description for this tile.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("{0}x-{1}y@{2}z", this.X, this.Y, this.Zoom);
        }

        /// <summary>
        /// Returns the top left corner.
        /// </summary>
        public Coordinate TopLeft
        {
            get
            {
                var n = System.Math.PI - ((2.0 * System.Math.PI * (double)this.Y) / System.Math.Pow(2.0, (double)this.Zoom));

                var longitude = (float)(((double)this.X / System.Math.Pow(2.0, (double)this.Zoom) * 360.0) - 180.0);
                var latitude = (float)(180.0 / System.Math.PI * System.Math.Atan(System.Math.Sinh(n)));

                return new Coordinate(latitude, longitude);
            }
        }

        /// <summary>
        /// Returns the bottom right corner.
        /// </summary>
        public Coordinate BottomRight
        {
            get
            {
                var n = System.Math.PI - ((2.0 * System.Math.PI * (this.Y + 1)) / System.Math.Pow(2.0, this.Zoom));

                var longitude = (float)(((this.X + 1) / System.Math.Pow(2.0, this.Zoom) * 360.0) - 180.0);
                var latitude = (float)(180.0 / System.Math.PI * System.Math.Atan(System.Math.Sinh(n)));

                return new Coordinate(latitude, longitude);
            }
        }

        /// <summary>
        /// Returns the 4 subtiles.
        /// </summary>
        /// <returns></returns>
        public TileRange SubTiles
        {
            get
            {
                return new TileRange(2 * this.X,
                    2 * this.Y,
                    2 * this.X + 1,
                    2 * this.Y + 1,
                    this.Zoom + 1);
            }
        }

        /// <summary>
        /// Returns the subtiles of this tile at the given zoom.
        /// </summary>
        /// <param name="zoom"></param>
        /// <returns></returns>
        public TileRange GetSubTiles(int zoom)
        {
            if (this.Zoom > zoom) { throw new ArgumentOutOfRangeException("zoom", "Subtiles can only be calculated for higher zooms."); }

            if (this.Zoom == zoom)
            { // just return a range of one tile.
                return new TileRange(this.X, this.Y, this.X, this.Y, this.Zoom);
            }

            var factor = 1 << (zoom - this.Zoom);

            return new TileRange(
                this.X * factor,
                this.Y * factor,
                this.X * factor + factor - 1,
                this.Y * factor + factor - 1,
                zoom);
        }

        /// <summary>
        /// Returns true if this tile overlaps the given tile.
        /// </summary>
        public bool Overlaps(Tile tile)
        {
            if (tile.Zoom == this.Zoom)
            { // only overlaps when identical.
                return tile.Equals(this);
            }
            else if (tile.Zoom > this.Zoom)
            { // the zoom is bigger.
                var range = this.GetSubTiles(tile.Zoom);
                return range.Contains(tile);
            }
            return false;
        }

        /// <summary>
        /// Returns true if this tile is completely overlapped by tiles in the given collection.
        /// </summary>
        /// <param name="tiles"></param>
        /// <returns></returns>
        public bool IsOverlappedBy(IEnumerable<Tile> tiles)
        {
            var foundTiles = new Dictionary<int, HashSet<Tile>>();
            foreach (Tile tile in tiles)
            {
                if (tile.Zoom <= this.Zoom)
                { // check regular overlaps.
                    if (tile.Overlaps(this))
                    { // ok, this collection overlaps this tile.
                        return true;
                    }
                }
                else
                { // tile is at a higher zoom level but several tiles combined can still overlap this one.
                    HashSet<Tile> found;
                    if (!foundTiles.TryGetValue(tile.Zoom, out found))
                    { // create new hashset.
                        found = new HashSet<Tile>();
                        foundTiles.Add(tile.Zoom, found);
                    }
                    found.Add(tile);
                }
            }

            // ok still no conclusive answer, check found tiles.
            foreach (var foundTilePair in foundTiles)
            {
                var subtiles = this.GetSubTiles(foundTilePair.Key);
                int count = 0;
                foreach (var foundTile in foundTilePair.Value)
                {
                    if (subtiles.Contains(foundTile))
                    { // the tile is in the subtiles collection.
                        count++;
                    }
                }
                if (subtiles.Count == foundTilePair.Value.Count)
                { // if the match is exact all subtiles are covered.
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Calculates the tile id of the tile at position (0, 0) for the given zoom.
        /// </summary>
        /// <param name="zoom"></param>
        /// <returns></returns>
        private static ulong CalculateTileId(int zoom)
        {
            if (zoom == 0)
            { // zoom level 0: {0}.
                return 0;
            }
            else if (zoom == 1)
            {
                return 1;
            }
            else if (zoom == 2)
            {
                return 5;
            }
            else if (zoom == 3)
            {
                return 21;
            }
            else if (zoom == 4)
            {
                return 85;
            }
            else if (zoom == 5)
            {
                return 341;
            }
            else if (zoom == 6)
            {
                return 1365;
            }
            else if (zoom == 7)
            {
                return 5461;
            }
            else if (zoom == 8)
            {
                return 21845;
            }
            else if (zoom == 9)
            {
                return 87381;
            }
            else if (zoom == 10)
            {
                return 349525;
            }
            else if (zoom == 11)
            {
                return 1398101;
            }
            else if (zoom == 12)
            {
                return 5592405;
            }
            else if (zoom == 13)
            {
                return 22369621;
            }
            else if (zoom == 14)
            {
                return 89478485;
            }
            else if (zoom == 15)
            {
                return 357913941;
            }
            else if (zoom == 16)
            {
                return 1431655765;
            }
            else if (zoom == 17)
            {
                return 5726623061;
            }
            else if (zoom == 18)
            {
                return 22906492245;
            }

            ulong size = (ulong)System.Math.Pow(2, 2 * (zoom - 1));
            var tileId = Tile.CalculateTileId(zoom - 1) + size;
            return tileId;
        }

        /// <summary>
        /// Calculates the tile id of the tile at position (x, y) for the given zoom.
        /// </summary>
        /// <param name="zoom"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        private static ulong CalculateTileId(int zoom, int x, int y)
        {
            ulong id = Tile.CalculateTileId(zoom);
            long width = (long)System.Math.Pow(2, zoom);
            return id + (ulong)x + (ulong)(y * width);
        }

        /// <summary>
        /// Calculate the tile given the id.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static Tile CalculateTile(ulong id)
        {
            // find out the zoom level first.
            int zoom = 0;
            if (id > 0)
            { // only if the id is at least at zoom level 1.
                while (id >= Tile.CalculateTileId(zoom))
                {
                    // move to the next zoom level and keep searching.
                    zoom++;
                }
                zoom--;
            }

            // calculate the x-y.
            ulong local = id - Tile.CalculateTileId(zoom);
            ulong width = (ulong)System.Math.Pow(2, zoom);
            int x = (int)(local % width);
            int y = (int)(local / width);

            return new Tile(x, y, zoom);
        }

        /// <summary>
        /// Returns the id of this tile.
        /// </summary>
        public ulong Id
        {
            get { return _id; }
        }

        /// <summary>
        /// Returns true if this tile is valid.
        /// </summary>
        public bool IsValid
        {
            get
            {
                if (this.X >= 0 &&
                    this.Y >= 0 &&
                    this.Zoom >= 0)
                { // some are negative.
                    var size = System.Math.Pow(2, this.Zoom);
                    return this.X < size && this.Y < size;
                }
                return false;
            }
        }

        #region Conversion Functions

        /// <summary>
        /// Returns the tile at the given location at the given zoom.
        /// </summary>
        public static Tile CreateAroundLocation(double latitude, double longitude, int zoom)
        {
            var n = (int)System.Math.Floor(System.Math.Pow(2, zoom));

            var rad = latitude.ToRadians();

            var x = (int)(((longitude + 180.0f) / 360.0f) * (double)n);
            var y = (int)((1.0f - (System.Math.Log(System.Math.Tan(rad) + (1.0f / System.Math.Cos(rad))))
                / System.Math.PI) / 2f * (double)n);

            return new Tile(x, y, zoom);
        }

        /// <summary>
        /// Returns the tile at the given location at the given zoom.
        /// </summary>
        public static Tile CreateAroundLocation(Coordinate location, int zoom)
        {
            return Tile.CreateAroundLocation(location.Latitude, location.Longitude, zoom);
        }

        /// <summary>
        /// Inverts the X-coordinate.
        /// </summary>
        /// <returns></returns>
        public Tile InvertX()
        {
            var n = (int)System.Math.Floor(System.Math.Pow(2, this.Zoom));

            return new Tile(n - this.X - 1, this.Y, this.Zoom);
        }

        /// <summary>
        /// Inverts the Y-coordinate.
        /// </summary>
        /// <returns></returns>
        public Tile InvertY()
        {
            var n = (int)System.Math.Floor(System.Math.Pow(2, this.Zoom));

            return new Tile(this.X, n - this.Y - 1, this.Zoom);
        }
        
        #endregion
    }
}