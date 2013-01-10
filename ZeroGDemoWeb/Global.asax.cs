using System;
using System.Web.Http;
using System.Web.Mvc;
using ZeroG.Data.Object;

namespace ZeroGDemoWeb
{
    public class Globals
    {
        public const string ObjectNameSpace = "ZGDemo";
    }

    // Note: For instructions on enabling IIS6 or IIS7 classic mode, 
    // visit http://go.microsoft.com/?LinkId=9394801
    public class WebApiApplication : System.Web.HttpApplication
    {
        public static ObjectService ObjectService;

        private void _InitializeObjectService()
        {
            var dataPath = Server.MapPath("/App_Data/ObjectService");
            var config = new Config(dataPath);
            ObjectService = new ObjectService(config);

            // ensure that Object Store namespace exists
            if (!ObjectService.NameSpaceExists(Globals.ObjectNameSpace))
            {
                ObjectService.CreateNameSpace(
                    new ObjectNameSpaceConfig(Globals.ObjectNameSpace, "Demo Owner", "Demo Location", DateTime.Now));
            }

            // provision ActivityFeed Object Store
            var metadata = ActivityFeed.GetMetadata();
            if (!ObjectService.ObjectNameExists(metadata.NameSpace, metadata.ObjectName))
            {
                ObjectService.ProvisionObjectStore(metadata);

                // create some test records to query
                var actService = new ActivityFeed(ObjectService);
                
                DateTime dt = DateTime.UtcNow - new TimeSpan(5, 0, 0);

                for (int i = 0; i < 1000; i++)
                {
                    var actType = ActivityType.News;

                    // put a few different types of activity types in
                    if (0 != i && (0 == i % 5))
                    {
                        actType = ActivityType.Article;
                    }

                    actService.Add(new Activity()
                    {
                        ActivityType = actType,
                        Title = "Demo article #" + i,
                        Text = "Demo article text #" + i,
                        Link = "http://www.website" + i + ".com",
                        Image = "image" + i + ".png",
                        CreatedUTC = DateTime.UtcNow,
                        ExpiresUTC = dt.AddSeconds(i)
                    });
                }
            }
        }

        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();

            WebApiConfig.Register(GlobalConfiguration.Configuration);

            _InitializeObjectService();
        }

        protected void Application_End()
        {
            ObjectService.Dispose();
        }
    }
}