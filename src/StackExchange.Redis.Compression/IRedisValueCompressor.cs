namespace StackExchange.Redis.Compression
{
    public interface IRedisValueCompressor
    {
        void Compress(ref byte[] valueBlob);
        void Decompress(ref byte[] valueBlob);
    }
}