using Microsoft.ApplicationInsights;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using RCK.CloudPlatform.AXD365;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Interface;
using RCK.CloudPlatform.Common.Utilities;
using RCK.CloudPlatform.D365;
using RCK.CloudPlatform.Model;
using RCK.CloudPlatform.Model.Customer;
using RCK.CloudPlatform.Model.ERP;
using RCK.CloudPlatform.Model.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Enums;
using VSI.CloudPlatform.Common.Interfaces;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Db;
using VSI.CloudPlatform.Model.Common;
using VSI.CloudPlatform.Model.Jobs;
using VSI.Model;

namespace FunctionApp.ECOM_TO_D365.Customer
{
    public class FunctionHelper
    {
        internal static CreateCustomerParams GetCustomerRequestParams(FunctionParams functionParams)
        {
            var functionSettings = functionParams.Settings;
            var securityParams = JsonConvert.DeserializeObject<HttpSecurity>(functionSettings.GetValue("D365SecuritySettings"));

            return new CreateCustomerParams
            {
                D365RetailConnectivity = new D365RetailConnectivity
                {
                    AzAD = securityParams.Settings.GetValue("ADTenant"),
                    ClientId = securityParams.Settings.GetValue("ADClientAppId"),
                    ClientSeceret = securityParams.Settings.GetValue("ADClientAppSecret"),
                    D365Uri = securityParams.Settings.GetValue("ADResource"),
                    RetailServerUri = securityParams.Settings.GetValue("RCSUResource"),
                    OperatingUnitNumber = securityParams.Settings.GetValue("OUN")
                },
                AXResource = securityParams.Settings.GetValue("AXResource"),
                AXCustomerUpdateServiceUri = functionSettings.GetValue("AXCustomerUpdateServiceUri"),
                AXCustomerAffiliationServiceUri = functionSettings.GetValue("AXCustomerAffiliationServiceUri"),
                MagentoResponseServiceUri = functionSettings.GetValue("MagentoResponseServiceUri"),
                MagentoResponseBulkServiceUri = functionSettings.GetValue("MagentoResponseBulkServiceUri"),
                MagentoResponseServiceToken = functionSettings.GetValue("MagentoResponseServiceToken"),
                ServicebusConnectionString = functionSettings.GetValue("ServicebusConnectionString"),
                MessageBatchSize = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MessageBatchSize"), 100),
                MessagePumpTimeout = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MessagePumpTimeout"), 120),
                MessageReceiverTimeout = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MessageReceiverTimeout"), 20),
                MaxDegreeOfParallelism = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("MaxDegreeOfParallelism"), 10),
                ArchiveBlobContainer = Environment.GetEnvironmentVariable("ArchiveBlobContainer"),
                SourceTopic = functionSettings.GetValue("SourceTopic"),
                SourceTopicSubscription = functionSettings.GetValue("SourceTopicSubscription"),
                ProcessName = functionParams.TransactionStep,
                WorkfowName = functionParams.TransactionName,
                FunctionParams = functionParams
            };
        }

        internal async static Task ProcessAsync(CreateCustomerParams requestParams, TelemetryClient telemetryClient, FunctionParams functionParams, ICloudDb cloudDb, IBlob blob)
        {
            var startDate = DateTime.Now;
            var activeMessageCount = FunctionUtilities.GetMessageCount(requestParams.ServicebusConnectionString, requestParams.SourceTopic, requestParams.SourceTopicSubscription);
            var threadCount = FunctionUtilities.GetThreads(requestParams.MaxDegreeOfParallelism, activeMessageCount);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Thread Count {threadCount}, Message Count {activeMessageCount}");

            if (threadCount == 0)
            {
                return;
            }

            var entityPath = EntityNameHelper.FormatSubscriptionPath(requestParams.SourceTopic, requestParams.SourceTopicSubscription);
            var messageReceiver = new MessageReceiver(requestParams.ServicebusConnectionString, entityPath, ReceiveMode.PeekLock);
            var cancelationToken = new CancellationTokenSource(TimeSpan.FromSeconds(requestParams.MessagePumpTimeout));

            while (!cancelationToken.IsCancellationRequested)
            {
                var messages = await messageReceiver.ReceiveAsync(requestParams.MessageBatchSize, TimeSpan.FromSeconds(requestParams.MessageReceiverTimeout));
                if (messages == null || messages.Count() == 0)
                {
                    break;
                }

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Received batch count {messages.Count()}.");

                Parallel.ForEach(messages, new ParallelOptions { MaxDegreeOfParallelism = requestParams.MaxDegreeOfParallelism }, message =>
                {
                    var messageProperties = message.UserProperties;
                    var stage = JsonConvert.DeserializeObject<EDI_STAGING>(Encoding.Default.GetString(message.Body));

                    try
                    {
                        telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Downloading original message from blob...");

                        var blobUri = stage.StepsDetails.Where(key => key.StepName != functionParams.TransactionStep && key.Status != MessageStatus.ERROR && key.StepName != "Load" && key.StepName != "Receive")
                                                         .OrderByDescending(key => key.StepOrder).FirstOrDefault()?.FileUrl;

                        var originalMessage = CommonUtility.GetFromBlobAsync(blob, blobUri).GetAwaiter().GetResult();

                        telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Downloaded original message from blob.");

                        var erpCustomer = JsonConvert.DeserializeObject<ErpCustomer>(originalMessage);

                        ProcessCustomer(requestParams, erpCustomer, telemetryClient, stage, startDate, cloudDb, blob);

                        messageReceiver.CompleteAsync(message.SystemProperties.LockToken).GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        CommonUtility.LogErrorStageMessage(cloudDb, stage, functionParams, startDate, ex, true);
                    }
                });
            }

            await messageReceiver.CloseAsync();
        }

        private static void ProcessCustomer(CreateCustomerParams requestParams, ErpCustomer erpCustomer, TelemetryClient telemetryClient, EDI_STAGING stage, DateTime startDate, ICloudDb cloudDb, IBlob blob)
        {
            var updation = erpCustomer.RecordId != 0;
            var blobPath = string.Empty;

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Connecting to Retail Server...");

            var retailContext = RetailConnectivityController.ConnectToRetailServerAsync(requestParams.D365RetailConnectivity).GetAwaiter().GetResult();
            
            var Is_CustomerAffiliation = bool.Parse(erpCustomer.isCustomerAffiliation);
           

            var cosmosCustomers = GetCustomersFromCosmos(cloudDb, new List<string> { erpCustomer.Email });
            var erpcustomers = new List<ErpCustomer> {erpCustomer};

            SplitAddressesIncaseOfSameAddressForBothBillingAndShipping(erpcustomers, cosmosCustomers);

            if (updation)
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Updating customers {string.Join(",", erpcustomers.Select(c => c.EcomCustomerId))} to D365!");

                var (success, response) = CustomerUpdateToAXAsync(erpcustomers, requestParams, telemetryClient, stage, cloudDb, startDate).GetAwaiter().GetResult();
                if (Is_CustomerAffiliation)
                {
                    erpCustomer.AccountNumber = cosmosCustomers.FirstOrDefault().ErpCustomerAccountNumber;
                    var c_response = CustomerAffiliationUpdateToAXAsync(erpcustomers, requestParams, telemetryClient, stage, cloudDb, startDate).GetAwaiter().GetResult();
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Affiliation Added");

                }
                if (response != null)
                {
                    var axResponseLogs = response.InfoLog.Where(i => i.Success).ToList();

                    var magentoCustomers = erpcustomers.Where(c => axResponseLogs.Any(l => l.MagentoCustomerId == c.EcomCustomerId)).ToList();
                    var erpCustomersRequestsDict = MakeRequestForCosmosInUpdateCase(axResponseLogs, magentoCustomers);

                    var requestEmails = erpCustomersRequestsDict.Select(c => c.Value.Item1.Email).ToList();
                    var requestStores = erpCustomersRequestsDict.Select(c => c.Value.Item1.StoreId).ToList();

                    var existingCosmosCustomers = cosmosCustomers.Where(c => requestEmails.Contains(c.Email) && requestStores.Contains(c.StoreId.ToString())).ToList();

                    CustomerController.UpdateCustomerReferenceInCosmosDb(erpCustomersRequestsDict, cloudDb, existingCosmosCustomers);

                    dynamic magentoRequest = new
                    {
                        response = new
                        {
                            success = response.Success,
                            info_log = response.InfoLog.Select(i => new
                            {
                                customer_id = i.MagentoCustomerId,
                                erp_customer_id = i.ErpCustomerId,
                                addresses = i.Addresses.Where(ca => ca.MagentoAddressId != "N/A")?.Select(a => new
                                {
                                    address_id = a.MagentoAddressId,
                                    erp_address_id = a.ErpAddressId
                                })?.ToArray() ?? Array.Empty<dynamic>(),
                                success = i.Success,
                                exception = i.Exception
                            }).ToArray()
                        }
                    };

                    CustomerController.SendResponseToMagento(requestParams.MagentoResponseBulkServiceUri, requestParams.MagentoResponseServiceToken, magentoRequest, telemetryClient, stage.Transaction_Id);
                }

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Uploading original message to blob storage.");

                if (!success)
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Pushing EDI Error Message.");

                    CommonUtility.LogErrorStageMessage(cloudDb, stage, requestParams.FunctionParams, startDate, new Exception($"Some customer's failed to update in AX, We have sent the status to Ecom, those will include in next run. \nAX service response:\n {JsonConvert.SerializeObject(response)}"), false);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Pushed EDI Error Message.");
                }
                else
                {
                    blobPath = CommonUtility.UploadOutboundDataFilesOnBlobAsync(requestParams.FunctionParams, blob, $"{JsonConvert.SerializeObject(response)}", stage.Transaction_Id, requestParams.ArchiveBlobContainer).GetAwaiter().GetResult();

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Uploaded original message to blob storage.");
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Pushing EDI Success Message.");

                    CommonUtility.LogSuccessStageMessage(cloudDb, stage, requestParams.FunctionParams, startDate, OverallStatus.IMPORTED, MessageStatus.COMPLETED, blobPath);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Pushed EDI Success Message.");
                }
            }
            else
            {

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Checking customer {erpCustomer.Email} existance in COSMOS DB.");

                var cosmosCustomer = cosmosCustomers.Where(c => c.Email == erpCustomer.Email && c.StoreId == Convert.ToInt32(erpCustomer.StoreId)).FirstOrDefault();

                // Check 1: If customer exist on CL then use it

                if (cosmosCustomer != null)
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Customer {erpCustomer.EcomCustomerId} found in COSMOS with Erpkey {cosmosCustomer.ErpCustomerId}.");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Sending customer reference to ECOM.");

                    CustomerController.SendCustomerResponseToEcom(cosmosCustomer, new RCKCustomer {ErpCustomers = erpcustomers}, telemetryClient, requestParams.MagentoResponseServiceUri, requestParams.MagentoResponseServiceToken, stage.Transaction_Id);

                    blobPath = CommonUtility.UploadOutboundDataFilesOnBlobAsync(requestParams.FunctionParams, blob, $"Customer already exist in D365 with Account No. {cosmosCustomer.ErpCustomerAccountNumber}, Erp references shared with magento!", stage.Transaction_Id, requestParams.ArchiveBlobContainer).GetAwaiter().GetResult();
                }
                else
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Creating customer {erpCustomer.EcomCustomerId} to D365!");

                    var customerResponse = CustomerController.CreateAsync(retailContext, erpCustomer, telemetryClient, RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, stage.Transaction_Id).GetAwaiter().GetResult();
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Affiliation Added");

                    if (Is_CustomerAffiliation)
                    {
                        erpCustomer.AccountNumber = customerResponse.AccountNumber;
                        var response = CustomerAffiliationUpdateToAXAsync(erpcustomers, requestParams, telemetryClient, stage, cloudDb, startDate).GetAwaiter().GetResult();
                    }
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Creating customer {erpCustomer.EcomCustomerId} addresses to D365!");

                    var addressResponse = CustomerController.CreateAddressesAsync(retailContext, customerResponse, erpCustomer.Addresses.ToList(), telemetryClient, RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, stage.Transaction_Id).GetAwaiter().GetResult();

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Saving customer reference in COSMOS DB.");
                    customerResponse.CustomerAffiliations = erpCustomer.CustomerAffiliations;
                    customerResponse.Addresses.Clear();
                    customerResponse.Addresses = addressResponse;

                    CustomerController.SaveCustomerReferenceInCosmosDb(customerResponse, cloudDb);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Sending customer reference to ECOM.");

                    CustomerController.SendCustomerResponseToEcom(customerResponse, erpCustomer, telemetryClient, requestParams.MagentoResponseServiceUri, requestParams.MagentoResponseServiceToken, stage.Transaction_Id);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Uploading original message to blob storage.");

                    blobPath = CommonUtility.UploadOutboundDataFilesOnBlobAsync(requestParams.FunctionParams, blob, $"Customer created successfully to D365 with Account No. {customerResponse.AccountNumber}!", stage.Transaction_Id, requestParams.ArchiveBlobContainer).GetAwaiter().GetResult();

                    stage.KeyData1 = customerResponse.AccountNumber;
                }

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Uploaded original message to blob storage.");
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Pushing EDI Success Message.");

                CommonUtility.LogSuccessStageMessage(cloudDb, stage, requestParams.FunctionParams, startDate, OverallStatus.IMPORTED, MessageStatus.COMPLETED, blobPath);

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Pushed EDI Success Message.");
            }
        }

        private static Dictionary<string, (ErpCustomer, List<CosmosCustomerAddress>)> MakeRequestForCosmosInUpdateCase(List<AXUpdateCustomerResponseInfoLog> axResponseLogs, List<ErpCustomer> erpCustomers)
        {
            var cosmosCustomersDict = new Dictionary<string, (ErpCustomer, List<CosmosCustomerAddress>)>();

            foreach (var erpCustomer in erpCustomers)
            {
                var addressGroups = erpCustomer.Addresses.GroupBy(ad => ad.EcomAddressId);
                var customerAxResponseLog = axResponseLogs.Where(l => l.MagentoCustomerId == erpCustomer.EcomCustomerId).FirstOrDefault();
                var cosmosCustomers = new List<CosmosCustomerAddress>();

                foreach (var addressGroup in addressGroups)
                {
                    if (addressGroup.Count() > 1)
                    {
                        var billingAddress = customerAxResponseLog.Addresses.FirstOrDefault(ad => ad.MagentoAddressId == "N/A");
                        var shippingAddress = customerAxResponseLog.Addresses.FirstOrDefault(ad => ad.MagentoAddressId == addressGroup.Key);

                        if (shippingAddress != null)
                        {
                            cosmosCustomers.Add(new CosmosCustomerAddress
                            {
                                address_id = addressGroup.Key,
                                erp_address_id = shippingAddress.ErpAddressId,
                                billing = true,
                                shipping = true,
                                billingAddressId = billingAddress.ErpAddressId
                            });
                        }
                    }
                    else
                    {
                        var add = customerAxResponseLog.Addresses.FirstOrDefault(ad => ad.MagentoAddressId == addressGroup.Key);
                        var cAddress = new CosmosCustomerAddress
                        {
                            address_id = addressGroup.Key,
                            erp_address_id = add.ErpAddressId,
                            billing = add.AddressTypeValue == "1",
                            shipping = add.AddressTypeValue == "2"
                        };

                        cosmosCustomers.Add(cAddress);
                    }

                    cosmosCustomersDict.Add(addressGroup.Key, (erpCustomer, cosmosCustomers));
                }
            }

            return cosmosCustomersDict;
        }

        private static void SplitAddressesIncaseOfSameAddressForBothBillingAndShipping(List<ErpCustomer> erpCustomers, List<CosmosCustomer> cosmosCustomers)
        {
            foreach (var erpCustomer in erpCustomers)
            {
                var newAddresses = new List<ErpAddress>();
                var cosmosCustomer = cosmosCustomers?.FirstOrDefault(c => c.EcomCustomerId.ToString() == erpCustomer.EcomCustomerId && c.StoreId == Convert.ToInt32(erpCustomer.StoreId));

                foreach (var customerAddress in erpCustomer.Addresses)
                {
                    if (customerAddress.IsBilling && customerAddress.IsShipping)
                    {
                        // Billing address

                        var billingErpId = cosmosCustomer?.Adddresses?.FirstOrDefault(c => c.billing && c.shipping && c.address_id == customerAddress.EcomAddressId)?.billingAddressId;

                        newAddresses.Add(new ErpAddress
                        {
                            RecordId = string.IsNullOrEmpty(billingErpId) ? 0 : Convert.ToInt64(billingErpId),
                            EcomAddressId = customerAddress.EcomAddressId,
                            EntityName = "Address",
                            AddressTypeValue = 1,
                            Name = customerAddress.Name,
                            Street = customerAddress.Street,
                            State = customerAddress.State,
                            City = customerAddress.City,
                            ZipCode = customerAddress.ZipCode,
                            Phone = customerAddress.Phone,
                            IsPrimary = false,
                            ThreeLetterISORegionName = customerAddress.ThreeLetterISORegionName,
                            AddressExtensionProperties = new List<KeyValue> { new KeyValue { Key = "VSI_MAGENTOADDRESSID", Value = "N/A" } }
                        });

                        // Shipping address

                        newAddresses.Add(new ErpAddress
                        {
                            RecordId = customerAddress.RecordId,
                            EcomAddressId = customerAddress.EcomAddressId,
                            EntityName = "Address",
                            AddressTypeValue = 2,
                            Name = customerAddress.Name,
                            Street = customerAddress.Street,
                            State = customerAddress.State,
                            City = customerAddress.City,
                            ZipCode = customerAddress.ZipCode,
                            Phone = customerAddress.Phone,
                            IsPrimary = true,
                            ThreeLetterISORegionName = customerAddress.ThreeLetterISORegionName,
                            AddressExtensionProperties = customerAddress.AddressExtensionProperties
                        });
                    }
                    else
                    {
                        if (customerAddress.AddressTypeValue == 1)
                        {
                            var address = cosmosCustomer?.Adddresses?.FirstOrDefault(c => c.address_id == customerAddress.EcomAddressId);

                            var billingErpId = string.Empty;

                            if (address != null)
                            {
                                if (address.billing && address.shipping)
                                {
                                    billingErpId = address?.billingAddressId ?? string.Empty;
                                }
                                else
                                {
                                    billingErpId = address?.erp_address_id ?? string.Empty;
                                }
                            }

                            var shadowCopyOfCustomerAddress = JsonConvert.DeserializeObject<ErpAddress>(JsonConvert.SerializeObject(customerAddress));

                            shadowCopyOfCustomerAddress.RecordId = string.IsNullOrWhiteSpace(billingErpId) ? 0 : Convert.ToInt64(billingErpId);

                            newAddresses.Add(shadowCopyOfCustomerAddress);
                        }
                        else
                        {
                            newAddresses.Add(customerAddress);
                        }
                    }
                }

                erpCustomer.Addresses = newAddresses;
            }
        }

        public static List<CosmosCustomer> GetCustomersFromCosmos(ICloudDb cloudDb, List<string> emails)
        {
            var customers = cloudDb.ExecuteQueryForRawJson<CosmosCustomer>(RCKDbTables.IntegrationData, $"SELECT * FROM c WHERE c.Entity = '{Entities.Customer}' AND c.Email IN ({"'" + string.Join("','", emails) + "'"})");

            return customers;
        }

        private async static Task<(bool, AXResponse<AXUpdateCustomerResponseInfoLog>)> CustomerUpdateToAXAsync(List<ErpCustomer> erpCustomers, CreateCustomerParams requestParams, TelemetryClient telemetryClient, EDI_STAGING stage, ICloudDb cloudDb, DateTime startDate)
        {
            using (var client = new HttpClient())
            {
                #region Fetching Token

                AuthenticationResult authenticationResult;

                try
                {
                    var authenticationContext = new Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext(requestParams.D365RetailConnectivity.AzAD, false);
                    var creadential = new ClientCredential(requestParams.D365RetailConnectivity.ClientId, requestParams.D365RetailConnectivity.ClientSeceret);

                    authenticationResult = await authenticationContext.AcquireTokenAsync(requestParams.AXResource, creadential);
                }
                catch (Exception ex)
                {
                    var depthException = CommonUtility.GetDepthInnerException(ex);
                    throw new Exception($"Getting error while fetching token from D365!, Error: {depthException?.Message}");
                }

                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + authenticationResult.AccessToken);

                #endregion

                #region Making Http Request

                dynamic request = new
                {
                    _request = new
                    {
                        Customer = erpCustomers.Select(c => new
                        {
                            Company = "rck",
                            CustomerRecId = c.RecordId,
                            Name = c.Name,
                            FirstName = c.FirstName,
                            LastName = c.LastName,
                            Phone = c.Phone,
                            Addresses = c.Addresses.Select(a => new
                            {
                                AddressTypeValue = a.AddressTypeValue,
                                AddressRecId = a.RecordId,
                                MagentoId = a.EcomAddressId,
                                AddressName = a.Name,
                                Street = a.Street,
                                State = a.State,
                                City = a.City,
                                ZipCode = a.ZipCode,
                                Phone = a.Phone,
                                ThreeLetterISORegionName = a.ThreeLetterISORegionName,
                                IsPrimary = a.IsPrimary,
                                AddressExtensionProperties = a.AddressExtensionProperties
                            }).ToList()
                        }).ToList()
                    }
                };

                #endregion

                #region Calling Update Request

                var updateResponse = await client.PostAsync(requestParams.AXCustomerUpdateServiceUri, new StringContent(JsonConvert.SerializeObject(request), Encoding.UTF8, "application/json"));

                var updateResponseString = await updateResponse.Content.ReadAsStringAsync();

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Magento service response: {updateResponseString}");

                if (updateResponse.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    telemetryClient.TrackException(new Exception(updateResponseString));

                    throw new Exception($"Getting error while updating customer in AX, Error: {updateResponseString}");
                }
                else
                {
                    var axResponse = JsonConvert.DeserializeObject<AXResponse<AXUpdateCustomerResponseInfoLog>>(updateResponseString);

                    axResponse.InfoLog = axResponse.InfoLog.Where(i => !i.Addresses.Any(ad => ad.MagentoAddressId == string.Empty)).ToList();

                    if (!axResponse.Success)
                    {
                        telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Api error response: {updateResponseString}");

                        CommonUtility.LogErrorStageMessage(cloudDb, stage, requestParams.FunctionParams, startDate, new Exception(updateResponseString), true);

                        return (false, null);
                    }

                    if (axResponse.InfoLog.Count > 0 && axResponse.InfoLog.Any(i => !i.Success))
                    {
                        telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Api error response: {updateResponseString}");

                        return (false, axResponse);
                    }

                    return (true, axResponse);
                }

                #endregion
            }
        }
        private async static Task<string> CustomerAffiliationUpdateToAXAsync(List<ErpCustomer> erpCustomers, CreateCustomerParams requestParams, TelemetryClient telemetryClient, EDI_STAGING stage, ICloudDb cloudDb, DateTime startDate)
        {

            using (var client = new HttpClient())
            {
                #region Fetching Token

                AuthenticationResult authenticationResult;

                try
                {
                    var authenticationContext = new Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext(requestParams.D365RetailConnectivity.AzAD, false);
                    var creadential = new ClientCredential(requestParams.D365RetailConnectivity.ClientId, requestParams.D365RetailConnectivity.ClientSeceret);

                    authenticationResult = await authenticationContext.AcquireTokenAsync(requestParams.AXResource, creadential);
                }
                catch (Exception ex)
                {
                    var depthException = CommonUtility.GetDepthInnerException(ex);
                    throw new Exception($"Getting error while fetching token from D365!, Error: {depthException?.Message}");
                }

                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + authenticationResult.AccessToken);

                #endregion

                #region Making Http Request

                dynamic request = new
                {
                    _request = new
                    {
                        Customer = erpCustomers.Select(c => new
                        {
                            Company = "rck",
                            CustomerAccountNumber = c.AccountNumber,

                            CustomerAffialtions = c.CustomerAffiliations.Select(a => new
                            {
                                Name = a.Name
                            }).ToList()
                        }).ToList()
                    }
                };

                #endregion

                #region Calling Update Request
                var updateResponse = await client.PostAsync(requestParams.AXCustomerAffiliationServiceUri, new StringContent(JsonConvert.SerializeObject(request), Encoding.UTF8, "application/json"));

                var updateResponseString = await updateResponse.Content.ReadAsStringAsync();

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {stage.Transaction_Id}, Magento service response: {updateResponseString}");

                if (updateResponse.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    telemetryClient.TrackException(new Exception(updateResponseString));

                    throw new Exception($"Getting error while updating customer in AX, Error: {updateResponseString}");
                }
                else
                {

                    return updateResponseString;

                    #endregion
                }
            }
        }
    }
}