#region License, Terms and Conditions
// Copyright (c) 2012 Jeremy Burman
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
namespace ZeroG.Data.Object
{
    class LocalObjectServiceAdminClient : IObjectServiceAdminClient
    {
        private ObjectService _service;
        private string _nameSpace;

        public LocalObjectServiceAdminClient(ObjectService service, string nameSpace)
        {
            if (null == service) { throw new ArgumentNullException("service"); }
            if (null == nameSpace) { throw new ArgumentNullException("nameSpace"); }

            _service = service;
            _nameSpace = nameSpace;
        }

        public void ProvisionObjectStore(ObjectMetadata metadata)
        {
            _service.ProvisionObjectStore(metadata);
        }

        public void UnprovisionObjectStore(string objectName)
        {
            _service.UnprovisionObjectStore(_nameSpace, objectName);
        }

        public ObjectMetadata GetObjectMetadata(string objectName)
        {
            return _service.GetObjectMetadata(_nameSpace, objectName);
        }

        public bool ObjectNameExists(string objectName)
        {
            return _service.ObjectNameExists(_nameSpace, objectName);
        }

        public void Truncate(string objectName, bool resetIdentifiers)
        {
            _service.Truncate(_nameSpace, objectName, resetIdentifiers);
        }
    }
}
