namespace StackExchange.Redis.Compression
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    public class RedisCompressionCommandHandler : IRedisCommandHandler
    {
        private bool CanBeExecuted(RedisKey[] involvedKeys = null)
        {
            return involvedKeys != null && involvedKeys.Length > 0 && RedisValueCompressor.KeyCanBeCompressed(involvedKeys.First());
        }

        protected virtual void CompressSET(RedisValue[] involvedValues)
        {
            byte[] valueBlob = involvedValues[0];
            RedisValueCompressor.Compressor.Compress(ref valueBlob);
            involvedValues[0] = valueBlob;
        }

        protected virtual void CompressValuesWithArgs(RedisValue[] involvedValues)
        {
            for (int i = 1; i < involvedValues.Length; i += 2)
            {
                byte[] valueBlob = involvedValues[i];
                RedisValueCompressor.Compressor.Compress(ref valueBlob);
                involvedValues[i] = valueBlob;
            }
        }

        protected virtual void CompressAdd(RedisValue[] involvedValues)
        {
            for (int i = 0; i < involvedValues.Length; i++)
            {
                byte[] valueBlob = involvedValues[i];
                RedisValueCompressor.Compressor.Compress(ref valueBlob);
                involvedValues[i] = valueBlob;
            }
        }

        public void OnExecuting(RedisCommand command, RedisKey[] involvedKeys = null, RedisValue[] involvedValues = null)
        {
            if (involvedValues != null && involvedValues.Length > 0 && CanBeExecuted(involvedKeys))
            {
                switch (command)
                {
                    case RedisCommand.SET:
                        CompressSET(involvedValues);
                        break;

                    case RedisCommand.HSET:
                    case RedisCommand.HMSET:
                    case RedisCommand.ZADD:
                        CompressValuesWithArgs(involvedValues);
                        break;

                    case RedisCommand.SADD:
                    case RedisCommand.LPUSH:
                    case RedisCommand.LPUSHX:
                        CompressAdd(involvedValues);
                        break;
                }
            }
        }

        public void OnExecuted(RedisCommand command, ref object result, RedisKey[] involvedKeys = null)
        {
            if (CanBeExecuted(involvedKeys))
            {
                if (result != null && result.GetType() != typeof(bool) && result.GetType() != typeof(int) && result.GetType() != typeof(long))
                {
                    bool isScanResult = command == RedisCommand.SCAN || command == RedisCommand.HSCAN || command == RedisCommand.SSCAN || command == RedisCommand.ZSCAN;
                    object scanValues = null;

                    if (isScanResult)
                        scanValues = result.GetType()
                                            .GetField("Values", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                                            .GetValue(result);

                    if (result.GetType() == typeof(RedisValue))
                    {
                        byte[] valueBlob = (RedisValue)result;
                        RedisValueCompressor.Compressor.Decompress(ref valueBlob);
                        result = (RedisValue)Encoding.UTF8.GetString(valueBlob);
                    }
                    else if (result.GetType() == typeof(RedisValue[]) || (command == RedisCommand.SCAN || command == RedisCommand.SSCAN))
                    {
                        RedisValue[] values;

                        if (!isScanResult)
                            values = (RedisValue[])result;
                        else
                            values = (RedisValue[])scanValues;

                        for (int i = 0; i < values.Length; i++)
                        {
                            byte[] valueBlob = values[i];
                            RedisValueCompressor.Compressor.Decompress(ref valueBlob);
                            values[i] = valueBlob;
                        }
                    }
                    else if (result.GetType() == typeof(SortedSetEntry))
                    {
                        SortedSetEntry source = (SortedSetEntry)result;
                        byte[] valueBlob = source.Element;
                        RedisValueCompressor.Compressor.Decompress(ref valueBlob);
                        result = new SortedSetEntry((RedisValue)valueBlob, source.Score);
                    }
                    else if (result.GetType() == typeof(SortedSetEntry[]) || command == RedisCommand.ZSCAN)
                    {
                        SortedSetEntry[] entries;

                        if (!isScanResult)
                            entries = (SortedSetEntry[])result;
                        else
                            entries = (SortedSetEntry[])scanValues;

                        for (int i = 0; i < entries.Length; i += 2)
                        {
                            SortedSetEntry source = (SortedSetEntry)entries[i];
                            byte[] valueBlob = source.Element;
                            RedisValueCompressor.Compressor.Decompress(ref valueBlob);
                            entries[i] = new SortedSetEntry((RedisValue)valueBlob, source.Score);
                        }
                    }
                    else if (result.GetType() == typeof(HashEntry))
                    {
                        HashEntry source = (HashEntry)result;

                        byte[] valueBlob = source.Value;
                        RedisValueCompressor.Compressor.Decompress(ref valueBlob);
                        result = new HashEntry(source.Name, (RedisValue)valueBlob);
                    }
                    else if (result.GetType() == typeof(HashEntry[]) || command == RedisCommand.HSCAN)
                    {
                        HashEntry[] entries;

                        if (!isScanResult)
                            entries = (HashEntry[])result;
                        else
                            // TODO: Not the best solution... But I need to investigate further how to improve StackExchange.Redis
                            // to get these values directly...
                            entries = (HashEntry[])scanValues;

                        for (int i = 0; i < entries.Length; i++)
                        {
                            HashEntry source = entries[i];
                            byte[] valueBlob = source.Value;
                            RedisValueCompressor.Compressor.Decompress(ref valueBlob);
                            entries[i] = new HashEntry(source.Name, (RedisValue)valueBlob);
                        }
                    }
                }
            }
        }
    }
}