using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Newtonsoft.Json;
using RCK.CloudPlatform.Common;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Interface;
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Interfaces;
using VSI.CloudPlatform.Core.Blob;
using VSI.CloudPlatform.Core.Common;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Core.Telemetry;
using VSI.CloudPlatform.Db;
using VSI.CloudPlatform.Model.Jobs;

namespace FunctionApp.ECOM_TO_D365.Customer
{
    public static class FUNC_Customer
    {
        private static readonly bool excludeDependency = FunctionUtilities.GetBoolValue(Environment.GetEnvironmentVariable("ExcludeDependency"), false);
        private static readonly string instrumentationKey = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
        private static IBlob blob;
        private static ICloudDb cloudDb;

        static FUNC_Customer()
        {
            blob = new AzureBlob(Environment.GetEnvironmentVariable("StorageConnectionString"));
            cloudDb = new CosmosCloudDb(Environment.GetEnvironmentVariable("CosmosConnectionString"));

            BindingRedirectApplicationHelper.Startup();
        }

        [FunctionName("FUNC_Customer")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "POST")] HttpRequestMessage req)
        {
            IOperationHolder<RequestTelemetry> operation = null;
            TelemetryClient telemetryClient = null;

            try
            {
                var request = await req.Content.ReadAsStringAsync();
                var functionParams = JsonConvert.DeserializeObject<FunctionParams>(request);

                var instanceKey = $"{functionParams.TransactionName}_{functionParams.PartnerShipId}_{functionParams.TransactionStep}";

                telemetryClient = TelemetryFactory.GetInstance(instanceKey, instrumentationKey, excludeDependency);
                operation = telemetryClient.StartOperation<RequestTelemetry>(RCKFunctionNames.FUNC_RCK_MAPPER, Guid.NewGuid().ToString());

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Starting...");
                telemetryClient.AddDefaultProperties(functionParams);

                var requestParams = FunctionHelper.GetCustomerRequestParams(functionParams);

                await FunctionHelper.ProcessAsync(requestParams, telemetryClient, functionParams, cloudDb, blob);

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, "Finished.");
                telemetryClient.StopOperation(operation);

                return req.CreateResponse(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                telemetryClient.TrackException(ex);
                telemetryClient.StopOperation(operation);

                throw;
            }
        }
    }
}