namespace StackExchange.Redis.Compression
{
    public interface IRedisValueCompressor
    {
        bool Compress(ref byte[] valueBlob);
        bool Decompress(ref byte[] valueBlob);
    }
}