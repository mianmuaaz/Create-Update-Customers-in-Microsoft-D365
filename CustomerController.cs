using Microsoft.ApplicationInsights;
using Microsoft.Dynamics.Commerce.RetailProxy;
using Newtonsoft.Json;
using RCK.CloudPlatform.Common;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using RCK.CloudPlatform.D365;
using RCK.CloudPlatform.Model;
using RCK.CloudPlatform.Model.Customer;
using RCK.CloudPlatform.Model.ERP;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Enums;
using VSI.CloudPlatform.Db;
using VSI_RT = VSI.Commerce.RetailProxy;

namespace RCK.CloudPlatform.AXD365
{
    public class CustomerController
    {
        public async static Task<ErpCustomer> CreateAsync(D365RetailServerContext retailContext, ErpCustomer erpCustomer, TelemetryClient telemetryClient, string functionName, string stageTransactionId)
        {
            try
            {
                var entity = JsonConvert.DeserializeObject<Customer>(JsonConvert.SerializeObject(erpCustomer));
                if (Convert.ToBoolean(erpCustomer.isCustomerAffiliation))
                {
                    entity.CustomerAffiliations = null;

                }
                InitNullPrimitiveCollection(entity);
                AddHeaderExtensionProperties(entity, erpCustomer.HeaderExtensionProperties);
                AddHeaderAttributes(entity, erpCustomer.HeaderAttributes);

                var manager = retailContext.FactoryManager.GetManager<VSI_RT.ICustomerManager>();
                //if (erpCustomer.IsGuestCustomer == "1")
                //{
                //    entity.Addresses.Clear();
                //}
                entity.Addresses.Clear();
                var customerResponse = await manager.VSI_CreateCustomer(entity);


                telemetryClient.TrackTrace(functionName, $"Transaction Id: {stageTransactionId}, Customer created in D365 with Account # {customerResponse.AccountNumber}.");

                var erpCustomerResponse = JsonConvert.DeserializeObject<ErpCustomer>(JsonConvert.SerializeObject(customerResponse));

                erpCustomerResponse.EcomCustomerId = erpCustomer.EcomCustomerId;
                erpCustomerResponse.StoreId = erpCustomer.StoreId;

                return erpCustomerResponse;
            }
            catch (Exception exception)
            {
                throw CommonUtility.GetDepthInnerException(exception);
            }
        }

        public async static Task<List<ErpAddress>> CreateAddressesAsync(D365RetailServerContext retailContext, ErpCustomer erpCustomer, List<ErpAddress> erpAddresses, TelemetryClient telemetryClient, string functionName, string stageTransactionId)
        {
            try
            {
                var entity = JsonConvert.DeserializeObject<Customer>(JsonConvert.SerializeObject(erpCustomer));

                InitNullPrimitiveCollection(entity);

                var addresses = JsonConvert.DeserializeObject<List<Address>>(JsonConvert.SerializeObject(erpAddresses));

                InitNullPrimitiveCollection(addresses);
                AddAddressExtensionProperties(addresses, erpAddresses.ToList());

                var manager = retailContext.FactoryManager.GetManager<VSI_RT.ICustomerManager>();

                /*var addresseSplit = erpAddresses.Where(a => a.Street.Contains(",")).ToList();

                if (addresseSplit.Count > 0)
                {
                    foreach (var address in addresses)
                    {
                        var streetSplit = address.Street;
                        if (streetSplit.Contains(","))
                        {
                            address.Street = streetSplit.Split(',')[0];
                            address.StreetNumber = streetSplit.Split(',')[1];
                        }
                    }
                }*/

                var addressResponse = await manager.VSI_AddCustomerAddress(entity, addresses);

                telemetryClient.TrackTrace(functionName, $"Transaction Id: {stageTransactionId}, Customer addresses created in D365.");

                var erpAddressesResponse = JsonConvert.DeserializeObject<List<ErpAddress>>(JsonConvert.SerializeObject(addressResponse.Addresses));

                foreach (var address in erpAddresses)
                {
                    var erpAddressResponse = erpAddressesResponse.Where(a => a.AddressTypeValue == address.AddressTypeValue).FirstOrDefault();

                    erpAddressResponse.EcomAddressId = address.EcomAddressId;
                    erpAddressResponse.IsBilling = address.IsBilling;
                    erpAddressResponse.IsShipping = address.IsShipping;
                }

                return erpAddressesResponse;
            }
            catch (Exception exception)
            {
                throw CommonUtility.GetDepthInnerException(exception);
            }
        }

        public async static Task<List<ErpCustomer>> SearchCustomerAsync(D365RetailServerContext retailContext, string searchTerm, string searchField)
        {
            try
            {
                var manager = retailContext.FactoryManager.GetManager<ICustomerManager>();

                var customers = await manager.SearchByFields(new CustomerSearchByFieldCriteria
                {
                    Criteria = new ObservableCollection<CustomerSearchByFieldCriterion>
                    {
                        new CustomerSearchByFieldCriterion
                        {
                            SearchField = new CustomerSearchFieldType { Name = searchField },
                            SearchTerm =searchTerm
                        }
                    }
                }, new QueryResultSettings { Paging = new PagingInfo { Top = 1000, Skip = 0 } });

                var erpCustomers = JsonConvert.DeserializeObject<List<ErpCustomer>>(JsonConvert.SerializeObject(customers.Results));

                return erpCustomers;
            }
            catch (Exception exception)
            {
                throw CommonUtility.GetDepthInnerException(exception);
            }
        }

        public async static Task<ErpCustomer> GetByAccountNumberAsync(D365RetailServerContext retailContext, string accountNumber)
        {
            try
            {
                var manager = retailContext.FactoryManager.GetManager<ICustomerManager>();

                var customer = await manager.Read(accountNumber);

                return JsonConvert.DeserializeObject<ErpCustomer>(JsonConvert.SerializeObject(customer));
            }
            catch (Exception exception)
            {
                throw CommonUtility.GetDepthInnerException(exception);
            }
        }

        private static void AddHeaderExtensionProperties(Customer customer, List<KeyValue> properties)
        {
            var extensionProperties = new ObservableCollection<CommerceProperty>();
            foreach (var property in properties)
            {
                extensionProperties.Add(new CommerceProperty
                {
                    Key = property.Key,
                    Value = new CommercePropertyValue
                    {
                        StringValue = property.Value
                    }
                });
            }

            customer.ExtensionProperties = extensionProperties;
        }

        private static void AddHeaderAttributes(Customer customer, List<KeyValue> headerAttributes)
        {
            var attributes = new ObservableCollection<CustomerAttribute>();
            foreach (var headerAttribute in headerAttributes)
            {
                attributes.Add(new CustomerAttribute
                {
                    Name = headerAttribute.Key,
                    AttributeValue = new CommercePropertyValue
                    {
                        StringValue = headerAttribute.Value
                    }
                });
            }

            customer.Attributes = attributes;
        }

        private static void AddAddressExtensionProperties(List<Address> addresses, List<ErpAddress> erpAddresses)
        {
            foreach (var address in erpAddresses)
            {
                var properties = new ObservableCollection<CommerceProperty>();
                foreach (var property in address.AddressExtensionProperties)
                {
                    properties.Add(new CommerceProperty
                    {
                        Key = property.Key,
                        Value = new CommercePropertyValue
                        {
                            StringValue = property.Value
                        }
                    });
                }
                addresses.FirstOrDefault(a => a.AddressTypeValue == address.AddressTypeValue).ExtensionProperties = properties;
            }
        }

        private static void InitNullPrimitiveCollection(List<Address> addresses)
        {
            foreach (var address in addresses)
            {
                if (address.ExtensionProperties == null)
                {
                    address.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                }
            }
        }

        private static void InitNullPrimitiveCollection(Customer customer)
        {
            if (customer.ExtensionProperties == null)
            {
                customer.ExtensionProperties = new ObservableCollection<CommerceProperty>();
            }

            if (customer.CustomerAffiliations == null)
            {
                customer.CustomerAffiliations = new ObservableCollection<CustomerAffiliation>();
            }
            else
            {
                foreach (var affiliation in customer.CustomerAffiliations)
                {
                    if (affiliation.ExtensionProperties == null)
                    {
                        affiliation.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }
            }

            foreach (var address in customer.Addresses)
            {
                if (address.ExtensionProperties == null)
                {
                    address.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                }
            }

            if (customer.Attributes == null)
            {
                customer.Attributes = new ObservableCollection<CustomerAttribute>();
            }
            else
            {
                foreach (var attribute in customer.Attributes)
                {
                    if (attribute.ExtensionProperties == null)
                    {
                        attribute.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }
            }

            if (customer.Images == null)
            {
                customer.Images = new ObservableCollection<MediaLocation>();
            }
            else
            {
                foreach (var image in customer.Images)
                {
                    if (image.ExtensionProperties == null)
                    {
                        image.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }
            }

            if (customer.Contacts == null)
            {
                customer.Contacts = new ObservableCollection<ContactInfo>();
            }
            else
            {
                foreach (var contact in customer.Contacts)
                {
                    if (contact.ExtensionProperties == null)
                    {
                        contact.ExtensionProperties = new ObservableCollection<CommerceProperty>();
                    }
                }
            }
        }

        public static void SendResponseToMagento(string apiUri, string token, dynamic request, TelemetryClient telemetryClient, string transactionId)
        {
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("Authorization", $"Bearer {token}");

                var response = client.PostAsync(apiUri, new StringContent(JsonConvert.SerializeObject(request), Encoding.UTF8, "application/json")).GetAwaiter().GetResult();

                var responseString = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_ECOM_TO_D365_CUSTOMER, $"Transaction Id: {transactionId}, Magento service response: {responseString}");

                if (response.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    telemetryClient.TrackException(new Exception(responseString));
                }
            }
        }

        public static void SaveCustomerReferenceInCosmosDb(ErpCustomer erpCustomer, ICloudDb cloudDb)
        {
            var addresses = new List<CosmosCustomerAddress>();
            var affiliations = new List<ErpCustomerAffiliation>();
            affiliations = erpCustomer.CustomerAffiliations.ToList();
            var groupAddressesByMagento = erpCustomer.Addresses.GroupBy(a => a.EcomAddressId);

            foreach (var a in groupAddressesByMagento)
            {
                if (a.Count() > 1)
                {
                    var billingAddress = a.FirstOrDefault(ad => ad.AddressTypeValue == 1);
                    var shippingAddress = a.FirstOrDefault(ad => ad.AddressTypeValue == 2);

                    if (shippingAddress != null)
                    {
                        addresses.Add(new CosmosCustomerAddress
                        {
                            address_id = shippingAddress.EcomAddressId,
                            erp_address_id = shippingAddress.RecordId.ToString(),
                            billing = true,
                            shipping = true,
                            billingAddressId = (billingAddress.RecordId == 0) ? "0" : billingAddress.RecordId.ToString()
                        });
                    }
                }

                foreach (var address in a)
                {
                    addresses.Add(new CosmosCustomerAddress
                    {
                        address_id = address.EcomAddressId,
                        erp_address_id = address.RecordId.ToString(),
                        billing = address.AddressTypeValue == 1,
                        shipping = address.AddressTypeValue == 2
                    });
                }
            }

            var requst = new CosmosCustomer
            {
                Entity = Entities.Customer.ToString(),
                ErpCustomerId = erpCustomer.RecordId,
                EcomCustomerId = Convert.ToInt64(erpCustomer.EcomCustomerId),
                ErpCustomerAccountNumber = erpCustomer.AccountNumber,
                Email = erpCustomer.Email,
                Adddresses = addresses,
                CustomerAffiliations = affiliations,
                StoreId = Convert.ToInt32(erpCustomer.StoreId)
            };

            IntegrationManager.SaveIntegrationKey(cloudDb, requst);
        }

        public static void UpdateCustomerReferenceInCosmosDb(Dictionary<string, (ErpCustomer, List<CosmosCustomerAddress>)> customersKeyPairs, ICloudDb cloudDb, List<CosmosCustomer> cosmosCustomers)
        {
            foreach (var customerKeyPair in customersKeyPairs)
            {
                var erpCustomer = customerKeyPair.Value.Item1;
                var cosmosDbCustomersAddresses = customerKeyPair.Value.Item2;
                var request = cosmosCustomers.Where(cc => cc.Email == erpCustomer.Email).FirstOrDefault();

                var diffExistingCosmosAddresses = request.Adddresses.Where(ad => !cosmosDbCustomersAddresses.Any(a => ad.address_id == a.address_id)).ToList();

                request.Adddresses = cosmosDbCustomersAddresses;

                if (diffExistingCosmosAddresses.Count > 0)
                {
                    request.Adddresses.AddRange(diffExistingCosmosAddresses);
                }

                CosmosHelper.Update(cloudDb, request, RCKDbTables.IntegrationData);
            }
        }

        public static void SendCustomerResponseToEcom(ErpCustomer erpCustomerResponse, ErpCustomer rckMagentoCustomer, TelemetryClient telemetryClient, string apiUri, string apiToken, string transactionId)
        {
            var addresses = new List<dynamic>();

            #region Same Billing/Shipping Case

            var sameBillingOrShippingAddresses = rckMagentoCustomer.Addresses.Where(c => c.IsBilling && c.IsShipping).ToList();

            foreach (var sameBillingOrShippingAddress in sameBillingOrShippingAddresses)
            {
                var shippingErpAddressId = erpCustomerResponse.Addresses.FirstOrDefault(c => c.EcomAddressId == sameBillingOrShippingAddress.EcomAddressId && c.AddressTypeValue == 2)?.RecordId ?? 0;

                addresses.Add(new
                {
                    address_id = sameBillingOrShippingAddress.EcomAddressId,
                    erp_address_id = shippingErpAddressId,
                    billing = true,
                    shipping = true
                });
            }

            #endregion

            #region Seperate Billing/Shipping Case

            var seperateBillingOrShippingAddresses = erpCustomerResponse.Addresses.Where(c => !sameBillingOrShippingAddresses.Any(sbs => c.EcomAddressId == sbs.EcomAddressId)).ToList();

            foreach (var address in seperateBillingOrShippingAddresses)
            {
                if (address.AddressTypeValue == 1)
                {
                    addresses.Add(new
                    {
                        address_id = address.EcomAddressId,
                        erp_address_id = address.RecordId,
                        billing = true
                    });
                }

                if (address.AddressTypeValue == 2)
                {
                    addresses.Add(new
                    {
                        address_id = address.EcomAddressId,
                        erp_address_id = address.RecordId,
                        shipping = true
                    });
                }
            }

            #endregion

            dynamic magentoRequest = new
            {
                response = new
                {
                    customer_id = erpCustomerResponse.EcomCustomerId,
                    erp_customer_id = erpCustomerResponse.RecordId,
                    addresses = addresses
                }
            };

            SendResponseToMagento(apiUri, apiToken, magentoRequest, telemetryClient, transactionId);
        }

        public static void SendCustomerResponseToEcom(CosmosCustomer cosmosCustomer, RCKCustomer rocklerCustomers, TelemetryClient telemetryClient, string apiUri, string apiToken, string transactionId)
        {
            var addresses = new List<dynamic>();

            #region Same Billing/Shipping Case

            var rckCustomer = rocklerCustomers.ErpCustomers.FirstOrDefault();
            var sameBillingOrShippingAddresses = rckCustomer.Addresses.Where(c => c.IsBilling && c.IsShipping).ToList();

            foreach (var sameBillingOrShippingAddress in sameBillingOrShippingAddresses)
            {
                var shippingErpAddressId = cosmosCustomer.Adddresses.FirstOrDefault(c => c.address_id == sameBillingOrShippingAddress.EcomAddressId && c.shipping)?.erp_address_id;

                addresses.Add(new
                {
                    address_id = sameBillingOrShippingAddress.EcomAddressId,
                    erp_address_id = shippingErpAddressId,
                    billing = true,
                    shipping = true
                });
            }

            #endregion

            #region Seperate Billing/Shipping Case

            var seperateBillingOrShippingAddresses = cosmosCustomer.Adddresses.Where(c => !sameBillingOrShippingAddresses.Any(sbs => c.address_id == sbs.EcomAddressId)).ToList();

            addresses.AddRange(seperateBillingOrShippingAddresses);

            #endregion

            dynamic magentoRequest = new
            {
                response = new
                {
                    customer_id = cosmosCustomer.EcomCustomerId,
                    erp_customer_id = cosmosCustomer.ErpCustomerId,
                    addresses = addresses
                }
            };

            SendResponseToMagento(apiUri, apiToken, magentoRequest, telemetryClient, transactionId);
        }
    }
}