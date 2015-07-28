namespace StackExchange.Redis.Compression
{
    using System.IO;
    using System.Linq;
    using System.IO.Compression;

    public sealed class GZipRedisValueCompressor : RedisValueCompressor
    {
        static GZipRedisValueCompressor()
        {
            gzipHeader = new byte[] { 0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 4, 0 };
        }

        internal static readonly byte[] gzipHeader;

        public override bool Compress(ref byte[] value)
        {
            bool compressed = false;

            if (value != null && value.Length >= RedisValueCompressor.CompressionThreshold && !value.SequenceEqual(gzipHeader))
                using (MemoryStream outputStream = new MemoryStream())
                {
                    using (MemoryStream inputStream = new MemoryStream(value))
                    using (GZipStream gzipStream = new GZipStream(outputStream, CompressionMode.Compress, true))
                    {
                        inputStream.CopyTo(gzipStream);
                        gzipStream.Close();
                    }

                    value = outputStream.ToArray();

                    compressed = true;
                }

            return compressed;
        }

        public override bool Decompress(ref byte[] value)
        {
            bool decompressed = false;

            if (value != null && value.Length > 0 && value.SequenceEqual(gzipHeader))
                using (MemoryStream inputStream = new MemoryStream(value))
                using (MemoryStream outputStream = new MemoryStream())
                {
                    using (GZipStream gzipStream = new GZipStream(inputStream, CompressionMode.Decompress, true))
                        gzipStream.CopyTo(outputStream);

                    value = outputStream.ToArray();
                    decompressed = true;
                }

            return decompressed;
        }
    }
}