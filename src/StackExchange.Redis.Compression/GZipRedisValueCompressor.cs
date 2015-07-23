namespace StackExchange.Redis.Compression
{
    using System.IO;
    using System.IO.Compression;

    public sealed class GZipRedisValueCompressor : RedisValueCompressor
    {
        public override void Compress(ref byte[] value)
        {
            using (MemoryStream outputStream = new MemoryStream())
            {
                using (MemoryStream inputStream = new MemoryStream(value))
                using (GZipStream gzipStream = new GZipStream(outputStream, CompressionMode.Compress, true))
                {
                    inputStream.CopyTo(gzipStream);
                    gzipStream.Close();
                }

                value = outputStream.ToArray();
            }
        }

        public override void Decompress(ref byte[] value)
        {
            using (MemoryStream inputStream = new MemoryStream(value))
            using (MemoryStream outputStream = new MemoryStream())
            {
                using (GZipStream gzipStream = new GZipStream(inputStream, CompressionMode.Decompress, true))
                {
                    gzipStream.CopyTo(outputStream);
                }

                value = outputStream.ToArray();
            }
        }
    }
}