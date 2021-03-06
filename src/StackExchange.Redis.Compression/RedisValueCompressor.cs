﻿namespace StackExchange.Redis.Compression
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public abstract class RedisValueCompressor : IRedisValueCompressor
    {
        static RedisValueCompressor()
        {
            KeyPatterns = new List<Func<string, bool>>();
            CompressionEnabled = true;
        }

        public static int CompressionThreshold { get; set; }
        public static List<Func<string, bool>> KeyPatterns { get; set; }
        public static IRedisValueCompressor Compressor { get; set; }
        public static bool CompressionEnabled { get; set; }

        public static bool UseDefaultCompressor
        {
            get
            {
                return Compressor != null && Compressor.GetType() == typeof(GZipRedisValueCompressor);
            }
            set
            {
                if (value)
                {
                    Compressor = new GZipRedisValueCompressor();
                }
            }
        }

        internal static bool KeyCanBeCompressed(RedisKey key)
        {
            return CompressionEnabled && (KeyPatterns == null || KeyPatterns.Count == 0 || KeyPatterns.Any(pattern => pattern(key)));
        }

        public abstract bool Compress(ref byte[] value);
        public abstract bool Decompress(ref byte[] value);
    }
}