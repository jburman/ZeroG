using Autofac;
using System;
using System.Configuration;
using ZeroG.Data;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Cache;
using ZeroG.Data.Object.Configure;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Tests
{
    public class ScopedTestContext : IDisposable
    {

        public const string ServiceName_ObjectServiceBuilderWithIndexCache = "objSvcBuilder_IndexCache";
        public const string ServiceName_ObjectServiceWithIndexCache = "objSvc_IndexCache";

        public ScopedTestContext(IScopedContainer container)
        {
            ScopedContainer = container;
        }

        public IScopedContainer ScopedContainer { get; private set; }

        public ObjectService GetObjectServiceWithIndexCache() =>
            ScopedContainer.ResolveNamed<ObjectService>(ServiceName_ObjectServiceWithIndexCache);

        public ObjectService GetObjectServiceWithoutIndexCache() =>
            ScopedContainer.Resolve<ObjectService>();

        public T Resolve<T>() => ScopedContainer.Resolve<T>();
        public T ResolveNamed<T>(string name) => ScopedContainer.ResolveNamed<T>(name);

        public void Dispose()
        {
            ScopedContainer.Dispose();
        }
    }

    public class TestContext
    {
        private static Lazy<TestContext> _instance = new Lazy<TestContext>();

        public static TestContext Instance
        {
            get => _instance.Value;
        }

        public static ScopedTestContext ScopedInstance
        {
            get => new ScopedTestContext(Instance.Container.BeginScope());
        }

        public IContainer Container { get; private set; }

        public TestContext()
        {
            _Init();
        }

        private void _Init()
        {
            Container = _ConfigureServices();
        }

        private IContainer _ConfigureServices()
        {
            var builder = new ContainerBuilder();

            var serializer = new ProtobufSerializer();

            var objectStoreOptions = new ObjectStoreOptions();

            // Object Service without Cache Options
            //var objSvcOptions = new ObjectServiceOptions();
            //objSvcOptions.WithKeyValueStoreProvider<RazorDBKeyValueStoreProvider>(
            //    new RazorDBKeyValueStoreProviderOptions(ConfigurationManager.AppSettings["ObjectServiceDataDir"],
            //    KeyValueCacheConfiguration.Shared,
            //    100 * 1024 * 1024))
            //    .WithObjectStoreOptions(objectStoreOptions)
            //    .WithSerializer(serializer);

            // Object Service with Index Cache
            var objSvcOptionsWithIndexCache = new ObjectServiceOptions();
            objSvcOptionsWithIndexCache.WithKeyValueStoreProvider<RazorDBKeyValueStoreProvider>(
                new RazorDBKeyValueStoreProviderOptions(ConfigurationManager.AppSettings["ObjectServiceDataDir"],
                KeyValueCacheConfiguration.Shared,
                100 * 1024 * 1024))
                .WithObjectIndexCache(new ObjectIndexCacheOptions())
                .WithObjectStoreOptions(objectStoreOptions)
                .WithSerializer(serializer);

            builder.Register(c => serializer).As<ISerializer>();
            //builder.Register(c => objSvcOptions);

            builder.Register(c =>
            {
                var objSvcOptions = new ObjectServiceOptions();
                objSvcOptions.WithKeyValueStoreProvider<RazorDBKeyValueStoreProvider>(
                    new RazorDBKeyValueStoreProviderOptions(ConfigurationManager.AppSettings["ObjectServiceDataDir"],
                    KeyValueCacheConfiguration.Shared,
                    100 * 1024 * 1024))
                    .WithObjectStoreOptions(objectStoreOptions)
                    .WithSerializer(serializer);
                return objSvcOptions;
            });

            builder.Register(c => c.Resolve<ObjectServiceOptions>()
                .GetKeyValueStoreProviderWithOptions((kvProviderType, options) =>
                {
                    var construct = kvProviderType.GetConstructor(new[] { typeof(KeyValueStoreProviderOptions) });
                    return (IKeyValueStoreProvider)construct.Invoke(new[] { options });
                })).As<IKeyValueStoreProvider>();

            builder.RegisterType<ObjectServiceBuilder>().SingleInstance();

            builder.RegisterType<ObjectServiceBuilder>()
                .Named<ObjectServiceBuilder>(ScopedTestContext.ServiceName_ObjectServiceBuilderWithIndexCache)
                .WithParameter(new PositionalParameter(0, objSvcOptionsWithIndexCache))
                .SingleInstance();

            builder.Register(c => new ObjectMetadataStore(serializer, objectStoreOptions.MaxObjectDependencies, c.Resolve<IKeyValueStoreProvider>()))
                .InstancePerLifetimeScope();
            builder.RegisterType<ObjectVersionStore>()
                .InstancePerLifetimeScope();
            builder.RegisterType<ObjectIndexerCache>()
                .InstancePerLifetimeScope();

            builder.Register(c => c.Resolve<ObjectServiceBuilder>().GetObjectService());
            builder.Register(c => c.ResolveNamed<ObjectServiceBuilder>(ScopedTestContext.ServiceName_ObjectServiceBuilderWithIndexCache).GetObjectService())
                .Named<ObjectService>(ScopedTestContext.ServiceName_ObjectServiceWithIndexCache);

            //builder.Register(c => objSvcOptions.GetVersionStore()).As<ObjectVersionStore>();
            //builder.Register(c => objSvcOptions.GetIDStore()).As<ObjectIDStore>();
            //builder.Register(c => objSvcOptions.GetObjectService()).As<IObjectStore>();
            //builder.Register(c => objSvcOptions.GetMetadataStore()).As<ObjectMetadataStore>();

            var container = builder.Build();
            return new DefaultContainer(container);
        }

        private class DefaultContainer : IContainer
        {
            private Autofac.IContainer _wrappedContainer;

            public DefaultContainer(Autofac.IContainer wrappedContainer)
            {
                _wrappedContainer = wrappedContainer;
            }

            public T Resolve<T>() => _wrappedContainer.Resolve<T>();
            public T ResolveNamed<T>(string name) => _wrappedContainer.ResolveNamed<T>(name);
            public IScopedContainer BeginScope() => new LifetimeScopedContainer(_wrappedContainer.BeginLifetimeScope());
        }

        private class LifetimeScopedContainer : IScopedContainer
        {
            private ILifetimeScope _wrappedScope;

            public LifetimeScopedContainer(ILifetimeScope wrappedScope)
            {
                _wrappedScope = wrappedScope;
            }

            public void Dispose() => _wrappedScope.Dispose();
            public T Resolve<T>() => _wrappedScope.Resolve<T>();
            public T ResolveNamed<T>(string name) => _wrappedScope.ResolveNamed<T>(name);
            public IScopedContainer BeginScope() => new LifetimeScopedContainer(_wrappedScope.BeginLifetimeScope());
        }
    }
}
