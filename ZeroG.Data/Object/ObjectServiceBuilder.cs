#region License, Terms and Conditions
// Copyright (c) 2017 Jeremy Burman
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion
using System;
using ZeroG.Data.Object.Cache;
using ZeroG.Data.Object.Configure;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object
{
    public class ObjectServiceBuilder
    {
        private ObjectServiceOptions _options;
        private IKeyValueStoreProvider _kvProvider;

        public ObjectServiceBuilder(ObjectServiceOptions options)
        {
            _options = options ?? new ObjectServiceOptions();

            _Initialize();
        }
        
        private void _Initialize()
        {
            _kvProvider = _options.GetKeyValueStoreProviderWithOptions(
                (kvProviderType, options) =>
                {
                    var construct = kvProviderType.GetConstructor(new[] { typeof(KeyValueStoreProviderOptions) });
                    return (IKeyValueStoreProvider)construct.Invoke(new[] { options });
                });
        }

        public ObjectService GetObjectService()
        {
            var scope = new ObjectServiceLifetimeScope();
            
            try
            {
                var objectStoreOptions = _options.GetObjectStoreOptions();
                var serializer = _options.GetSerializer();

                var metadata = GetMetadataStore(serializer, objectStoreOptions);
                scope.Add(metadata);

                var naming = GetObjectNaming(metadata);
                var idStore = GetIDStore(serializer);
                scope.Add(idStore);

                var versions = GetVersionStore(serializer, metadata);
                scope.Add(versions);

                var store = GetObjectStore(serializer, metadata, objectStoreOptions);
                scope.Add(store);

                var indexer = GetObjectIndexer(scope, metadata, versions);
                scope.Add(indexer);

                return new ObjectService(scope,
                    serializer,
                    _kvProvider,
                    metadata,
                    naming,
                    idStore,
                    versions,
                    _options.GetObjectVersionChangeHandler(),
                    indexer,
                    store);
            }
            catch
            {
                scope.Dispose();
                throw;
            }
        }

        internal IObjectStore GetObjectStore(ISerializer serializer, ObjectMetadataStore objectMetadata, ObjectStoreOptions options) =>
            new ObjectStore(serializer, objectMetadata, _kvProvider, options.AutoClose, options.AutoCloseTimeout);

        internal ObjectMetadataStore GetMetadataStore(ISerializer serializer, ObjectStoreOptions options) =>
            new ObjectMetadataStore(serializer, options.MaxObjectDependencies, _kvProvider);

        internal ObjectNaming GetObjectNaming(ObjectMetadataStore objectMetadata) =>
            new ObjectNaming(objectMetadata);

        internal ObjectIDStore GetIDStore(ISerializer serializer) =>
            new ObjectIDStore(serializer, _kvProvider);

        internal ObjectVersionStore GetVersionStore(ISerializer serializer, ObjectMetadataStore objectMetadata) =>
            new ObjectVersionStore(serializer, objectMetadata, _kvProvider);

        internal ObjectIndexer GetObjectIndexer(ObjectServiceLifetimeScope scope, ObjectMetadataStore objectMetadata, ObjectVersionStore objectVersions)
        {
            ObjectIndexer indexer = null;
            var cacheOptions = _options.GetObjectObjectIndexCacheOptions();
            if(cacheOptions == null)
                indexer = new ObjectIndexer();
            else
            {
                var indexerCache = new ObjectIndexerCache(objectMetadata, objectVersions);
                scope.Add(indexerCache);

                var indexerCacheCleaner = new HardPruneCacheCleaner(indexerCache,
                    cacheOptions.MaxQueries,
                    cacheOptions.MaxValues,
                    HardPruneCacheCleaner.DefaultReductionFactor,
                    HardPruneCacheCleaner.DefaultCleanFrequency);

                indexer = new ObjectIndexer(indexerCache);
            }
            scope.Add(indexer);
            return indexer;
        }
    }
}
