namespace ZeroG.Data.Object.Configure
{
    public class ObjectIndexCacheOptions
    {
        public ObjectIndexCacheOptions(int maxQueries = 100_000, int maxValues = 10_000_000)
        {
            MaxQueries = maxQueries;
            MaxValues = maxValues;
        }

        public int MaxQueries { get; private set; }
        public int MaxValues { get; private set; }
    }
}
