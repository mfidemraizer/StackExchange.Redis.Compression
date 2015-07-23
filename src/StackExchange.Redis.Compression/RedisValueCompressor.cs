namespace StackExchange.Redis.Compression
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public abstract class RedisValueCompressor : IRedisValueCompressor
    {
        static RedisValueCompressor()
        {
            KeyPatterns = new List<Func<string, bool>>();
            MinimumPlainSizeToCompress = 32;
        }

        public static int MinimumPlainSizeToCompress { get; set; }
        public static List<Func<string, bool>> KeyPatterns { get; set; }
        public static IRedisValueCompressor Compressor { get; set; }

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
            return KeyPatterns == null || KeyPatterns.Count == 0 || KeyPatterns.Any(pattern => pattern(key));
        }

        public abstract void Compress(ref byte[] value);
        public abstract void Decompress(ref byte[] value);
    }
}