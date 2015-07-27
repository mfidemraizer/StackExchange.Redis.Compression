namespace StackExchange.Redis.Compression.Test
{
    using System;

    public abstract class TestBase
    {
        protected ConnectionMultiplexer CreateMultiplexer(Action<ConfigurationOptions> init = null)
        {
            ConfigurationOptions options = new ConfigurationOptions();
            options.AllowAdmin = true;
            options.EndPoints.Add("localhost:8512");

            if (init != null)
                init(options);

            return ConnectionMultiplexer.Connect(options);
        }

        protected IDatabase GetDatabase()
        {
            return CreateMultiplexer().GetDatabase(0);
        }

        protected void DropDatabase()
        {
            CreateMultiplexer().GetServer("localhost:8512").FlushAllDatabases();
        }
    }
}