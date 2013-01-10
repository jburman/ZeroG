using System.Web.Http;

namespace ZeroGDemoWeb
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            config.Routes.MapHttpRoute(
                name: "ActivityApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional,
                    type = ActivityType.News
                }
            );
        }
    }
}
