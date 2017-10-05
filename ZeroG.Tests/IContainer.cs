using System;

namespace ZeroG.Tests
{
    public interface IContainer
    {
        T Resolve<T>();
        T ResolveNamed<T>(string name);
        IScopedContainer BeginScope();
    }

    public interface IScopedContainer : IContainer, IDisposable
    {

    }
}
