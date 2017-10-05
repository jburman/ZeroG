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
using System.Collections.Generic;
using System.Diagnostics;

namespace ZeroG.Data.Object
{
    /// <summary>
    /// Wraps up Disposable objects related to a ObjectService instance created by 
    /// the ObjectServiceBuilder.
    /// All of these objects are created and should be disposed together.
    /// </summary>
    internal class ObjectServiceLifetimeScope : IDisposable
    {
        private List<IDisposable> _objects;

        public ObjectServiceLifetimeScope()
        {
            _objects = new List<IDisposable>();
        }

        public void Add(IDisposable nextObject)
        {
            if (!_objects.Contains(nextObject))
                _objects.Add(nextObject);
        }

        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    foreach(var dispose in _objects)
                    {
                        try
                        {
                            dispose.Dispose();
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine(ex.ToString());
                            string typeName = dispose?.GetType().Name ?? "<Unkown>";
                            Trace.TraceError("Unable to Dispose type " + typeName + " in " + nameof(ObjectServiceLifetimeScope) +
                                " : " + ex.Message);
                        }
                    }
                }
                disposedValue = true;
            }
        }

        public void Dispose() =>
            Dispose(true);
    }
}
