namespace StackExchange.Redis.Compression.Test
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;

    [TestClass]
    public class GzipRedisValueCompressorTest
    {
        [TestMethod]
        public void CanCompressAndDecompressSET()
        {
            RedisValueCompressor.UseDefaultCompressor = true;
            RedisCommand command = RedisCommand.SET;
            RedisKey testKey = "test";
            RedisValue[] testValue = new RedisValue[] { "hello world!" };

            RedisCompressionCommandHandler handler = new RedisCompressionCommandHandler();
            handler.OnExecuting(command, new[] { testKey }, testValue);

            object testResult = testValue[0];
            handler.OnExecuted(command, ref testResult, new[] { testKey });
        }
    }
}