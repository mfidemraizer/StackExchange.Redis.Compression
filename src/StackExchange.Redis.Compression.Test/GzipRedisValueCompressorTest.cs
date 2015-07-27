namespace StackExchange.Redis.Compression.Test
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    [TestClass]
    public class GzipRedisValueCompressorTest : TestBase
    {
        private static Process redisServer;

        [ClassInitialize]
        public static void ClassInit(TestContext context)
        {
            RedisServiceFactory.Register<IRedisCommandHandler, RedisCompressionCommandHandler>();

            RedisValueCompressor.UseDefaultCompressor = true;

            redisServer = Process.Start("redis-server.exe", "--port 8512 --maxheap 1g");
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            redisServer.Kill();
        }

        [TestInitialize]
        public void Init()
        {
            DropDatabase();
        }

        [TestMethod, DeploymentItem("redis-server.exe")]
        public void CanCompressAndDecompressString()
        {
            const string textToCompress = "hello world from this compressor!";

            IDatabase db = GetDatabase();

            db.StringSet("a", textToCompress);

            RedisValueCompressor.CompressionEnabled = false;

            string raw = db.StringGet("a");

            Assert.AreNotEqual(textToCompress, raw);

            RedisValueCompressor.CompressionEnabled = true;

            string plain = db.StringGet("a");

            Assert.AreEqual(textToCompress, plain);
        }

        [TestMethod]
        public void CanCompressAndDecompressHash()
        {
            const string textToCompress = "hello world from this compressor!";
            const string textToCompress2 = "hello world from this compressor +++++++++++++!";

            IDatabase db = GetDatabase();

            db.HashSet("test", new[] { new HashEntry("text1", textToCompress), new HashEntry("text2", textToCompress2) });

            RedisValueCompressor.CompressionEnabled = false;

            string raw = db.HashGet("test", "text1");
            string raw2 = db.HashGet("test", "text2");

            Assert.AreNotEqual(textToCompress, raw);
            Assert.AreNotEqual(textToCompress2, raw2);

            RedisValueCompressor.CompressionEnabled = true;

            string plain = db.HashGet("test", "text1");
            string plain2 = db.HashGet("test", "text2");
            HashEntry[] allPlains = db.HashGetAll("test");

            Assert.AreEqual(textToCompress, plain);
            Assert.AreEqual(textToCompress2, plain2);
            Assert.IsTrue(allPlains.Any(entry => entry.Name == "text1" && entry.Value == textToCompress));
            Assert.IsTrue(allPlains.Any(entry => entry.Name == "text2" && entry.Value == textToCompress2));
        }

        [TestMethod]
        public void CanReadCompressedHashScan()
        {
            const string textToCompress = "hello world from this compressor!";
            const string textToCompress2 = "hello world from this compressor +++++++++++++!";

            IDatabase db = GetDatabase();

            db.HashSet("test", new[] { new HashEntry("text1", textToCompress), new HashEntry("text2", textToCompress2) });

            RedisValueCompressor.CompressionEnabled = false;
            List<HashEntry> rawEntries = db.HashScan("test", "*").ToList();

            Assert.AreEqual(2, rawEntries.Count);
            Assert.IsFalse(rawEntries.Any(entry => entry.Name == "text1" && entry.Value == textToCompress));
            Assert.IsFalse(rawEntries.Any(entry => entry.Name == "text2" && entry.Value == textToCompress2));

            RedisValueCompressor.CompressionEnabled = true;
            List<HashEntry> plainEntries = db.HashScan("test", "*").ToList();

            Assert.AreEqual(2, plainEntries.Count);
            Assert.IsTrue(plainEntries.Any(entry => entry.Name == "text1" && entry.Value == textToCompress));
            Assert.IsTrue(plainEntries.Any(entry => entry.Name == "text2" && entry.Value == textToCompress2));
        }

        [TestMethod]
        public void CanCompressAndDecompressSetMembers()
        {
            const string textToCompress = "hello world from this compressor!";
            const string textToCompress2 = "hello world from this compressor +++++++++++++!";
            const string textToCompress3 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++++++++++!";
            const string textToCompress4 = "hello world from this compressor! ++++";
            const string textToCompress5 = "hello world from this compressor ++++++++++++++!";
            const string textToCompress6 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++!";
            const string textToCompress7 = "hello world from this compressor ++++++++++++++++++++++!";

            IDatabase db = GetDatabase();

            db.SetAdd("testSet", textToCompress);
            db.SetAdd("testSet", textToCompress2);
            db.SetAdd("testSet", textToCompress3);
            db.SetAdd("testSet", new RedisValue[] { textToCompress4, textToCompress5, textToCompress6, textToCompress7 });

            RedisValueCompressor.CompressionEnabled = false;

            RedisValue[] rawMembers = db.SetMembers("testSet");

            Assert.IsFalse(rawMembers.Any(raw => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == raw)));

            RedisValueCompressor.CompressionEnabled = true;

            RedisValue[] plainMembers = db.SetMembers("testSet");

            Assert.IsTrue(plainMembers.Any(plain => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == plain)));
        }

        [TestMethod]
        public void CanScanSetMembers()
        {
            const string textToCompress = "hello world from this compressor!";
            const string textToCompress2 = "hello world from this compressor +++++++++++++!";
            const string textToCompress3 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++++++++++!";
            const string textToCompress4 = "hello world from this compressor! ++++";
            const string textToCompress5 = "hello world from this compressor ++++++++++++++!";
            const string textToCompress6 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++!";
            const string textToCompress7 = "hello world from this compressor ++++++++++++++++++++++!";

            IDatabase db = GetDatabase();

            db.SetAdd("testSet", textToCompress);
            db.SetAdd("testSet", textToCompress2);
            db.SetAdd("testSet", textToCompress3);
            db.SetAdd("testSet", new RedisValue[] { textToCompress4, textToCompress5, textToCompress6, textToCompress7 });

            RedisValueCompressor.CompressionEnabled = false;

            RedisValue[] rawMembers = db.SetScan("testSet", "*", 999).ToArray();

            Assert.IsFalse(rawMembers.Any(raw => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == raw)));

            RedisValueCompressor.CompressionEnabled = true;

            RedisValue[] plainMembers = db.SetScan("testSet", "*", 999).ToArray();

            Assert.IsTrue(plainMembers.Any(plain => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == plain)));
        }

        [TestMethod]
        public void CanCompressAndDecompressSortedSetMembers()
        {
            const string textToCompress = "hello world from this compressor!";
            const string textToCompress2 = "hello world from this compressor +++++++++++++!";
            const string textToCompress3 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++++++++++!";
            const string textToCompress4 = "hello world from this compressor! ++++";
            const string textToCompress5 = "hello world from this compressor ++++++++++++++!";
            const string textToCompress6 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++!";
            const string textToCompress7 = "hello world from this compressor ++++++++++++++++++++++!";

            IDatabase db = GetDatabase();

            db.SortedSetAdd("testSortedSet", textToCompress, 1);
            db.SortedSetAdd("testSortedSet", textToCompress2, 2);
            db.SortedSetAdd("testSortedSet", textToCompress3, 3);
            db.SortedSetAdd("testSortedSet", new SortedSetEntry[] { new SortedSetEntry(textToCompress4, 4), new SortedSetEntry(textToCompress5, 5), new SortedSetEntry(textToCompress6, 6), new SortedSetEntry(textToCompress7, 7) });

            RedisValueCompressor.CompressionEnabled = false;

            RedisValue[] rawMembers = db.SortedSetRangeByRank("testSortedSet");

            Assert.IsFalse(rawMembers.Any(raw => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == raw)));

            RedisValueCompressor.CompressionEnabled = true;

            RedisValue[] plainMembers = db.SortedSetRangeByRank("testSortedSet");

            Assert.IsTrue(plainMembers.Any(plain => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == plain)));
        }

        [TestMethod]
        public void CanScanSortedSetMembers()
        {
            const string textToCompress = "hello world from this compressor!";
            const string textToCompress2 = "hello world from this compressor +++++++++++++!";
            const string textToCompress3 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++++++++++!";
            const string textToCompress4 = "hello world from this compressor! ++++";
            const string textToCompress5 = "hello world from this compressor ++++++++++++++!";
            const string textToCompress6 = "hello world from this compressor +++++++++++++++++++++++++++++++++++++!";
            const string textToCompress7 = "hello world from this compressor ++++++++++++++++++++++!";

            IDatabase db = GetDatabase();

            db.SortedSetAdd("testSortedSet", textToCompress, 1);
            db.SortedSetAdd("testSortedSet", textToCompress2, 2);
            db.SortedSetAdd("testSortedSet", textToCompress3, 3);
            db.SortedSetAdd("testSortedSet", new SortedSetEntry[] { new SortedSetEntry(textToCompress4, 4), new SortedSetEntry(textToCompress5, 5), new SortedSetEntry(textToCompress6, 6), new SortedSetEntry(textToCompress7, 7) });

            RedisValueCompressor.CompressionEnabled = false;

            RedisValue[] rawMembers = db.SortedSetScan("testSortedSet").Select(entry => entry.Element).ToArray();

            Assert.IsFalse(rawMembers.Any(raw => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == raw)));

            RedisValueCompressor.CompressionEnabled = true;

            RedisValue[] plainMembers = db.SortedSetScan("testSortedSet").Select(entry => entry.Element).ToArray();

            Assert.IsTrue(plainMembers.Any(plain => new[] { textToCompress, textToCompress2, textToCompress3, textToCompress4, textToCompress5, textToCompress6, textToCompress7 }.Any(text => text == plain)));
        }

        [TestMethod]
        public void CanSelectivelyCompressByKeyPatterns()
        {
            const string key1 = "test1";
            const string key2 = "test2";
            const string testVal1 = "hello world";
            const string testVal2 = "goodbye";

            RedisValueCompressor.KeyPatterns.Add(key => key.Contains("test"));

            IDatabase db = GetDatabase();
            db.StringSet(key1, testVal1);
            db.StringSet(key2, testVal2);

            RedisValueCompressor.CompressionEnabled = false;
            string val1 = db.StringGet(key1);
            string val2 = db.StringGet(key2);
            RedisValueCompressor.CompressionEnabled = true;

            Assert.AreNotEqual(testVal1, val1);
            Assert.AreNotEqual(testVal2, val2);

            db.StringSet("doh", "hahaha");
            RedisValueCompressor.CompressionEnabled = false;

            string notCompressed = db.StringGet("doh");

            Assert.AreEqual("hahaha", notCompressed);
        }
    }
}