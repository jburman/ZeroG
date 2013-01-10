using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace ZeroGDemoWeb.Controllers
{
    /// <summary>
    /// This controller demonstrates how the ZeroG Object Service can be interacted with from a web service.
    /// </summary>
    public class ActivityController : ApiController
    {
        private ActivityFeed _feed;

        public ActivityController()
        {
            _feed = new ActivityFeed(WebApiApplication.ObjectService);
        }

        /// <summary>
        /// Returns the most recent 100 activities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Activity> GetRecentActivity()
        {
            return _feed.Recent(100);
        }

        public Activity Get(int id)
        {
            return _feed.Get(id);
        }

        public IEnumerable<Activity> GetOfType(uint count, ActivityType type)
        {
            return _feed.RecentOfType(count, type);
        }

        public HttpResponseMessage Post(Activity activity)
        {
            if (ModelState.IsValid && null != activity)
            {
                activity.CreatedUTC = DateTime.UtcNow;

                _feed.Add(activity);

                var response = Request.CreateResponse<Activity>(HttpStatusCode.Created, activity);

                string uri = Url.Link("ActivityApi", new { id = activity.ID });
                response.Headers.Location = new Uri(uri);
                return response;
            }
            else
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest);
            }
        }

        public void Put(int id, Activity activity)
        {
            _feed.Update(id, activity);
        }

        public void Delete(int id)
        {
            _feed.Remove(id);
        }
    }
}