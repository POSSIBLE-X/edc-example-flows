"""
  Copyright 2024 Dataport. All rights reserved. Developed as part of the MERLOT project.
  
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
      http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""
from icecream import ic
import uuid
from common_v2 import create_asset, create_policy, create_contract_definition, query_catalog, \
    negotiate_offer, poll_negotiation_until_finalized, initiate_data_transfer, poll_transfer_until_completed, \
    create_s3_dataaddress, edc1_headers, edc2_headers

"""
Endpoint configuration
"""
consumer_connector_management_url = "http://localhost:8123/edc1/management/"
consumer_connector_dsp_url = "http://localhost:8123/edc1/protocol"

provider_connector_management_url = "http://localhost:8123/edc2/management/"
provider_connector_dsp_url = "http://localhost:8123/edc2/protocol"

"""
Create Asset
"""
# Provider
ic("Creating asset in provider connector")
asset_id = create_asset(str(uuid.uuid4()), "My Asset", "Description", "v1.2.3", "application/json",
                        {
                            "type": "IonosS3",
                            "keyName": "mykey123",
                            "storage": "s3-eu-central-1.ionoscloud.com",
                            "bucketName": "merlotedcprovider",
                            "blobName": "testfolder/"
                        },
                        #create_s3_dataaddress("testfolder/", "merlotedcprovider", "company1", "testfolder/",
                        #                      "testfolder/", "s3-eu-central-1.ionoscloud.com"),
                        provider_connector_management_url, edc2_headers)
ic(asset_id)

"""
Create Policy
"""

# Provider
ic("Creating policy in provider connector")
policy_id = create_policy(str(uuid.uuid4()), asset_id, provider_connector_management_url, edc2_headers)

"""
Create contract definition
"""

# Provider
ic("Create contract definition for the created asset and policy on provider connector")
create_contract_definition(policy_id, policy_id, asset_id, provider_connector_management_url, edc2_headers)

"""
Fetch catalog from provider
"""

# Consumer asks own connector to query providers catalog
ic("Query providers catalog")

offering_data = query_catalog(provider_connector_dsp_url, consumer_connector_management_url, edc1_headers)
if not isinstance(offering_data, dict):  # if there are multiple entries, choose the first one
    offering_data = offering_data[0]

"""
Negotiate contract from available offerings
"""
# Consumer asks own connector to negotiate with providers connector (repeating the offering)
ic("Negotiate offer")

negotiation_id = negotiate_offer("59:0B:DD:26:41:AC:57:D7:ED:76:D5:84:F8:BC:AC:8E:4C:C7:56:70:keyid:59:0B:DD:26:41:AC:57:D7:ED:76:D5:84:F8:BC:AC:8E:4C:C7:56:70",
                                  "20:1D:9C:04:0A:71:B9:E7:8C:28:9D:70:A6:84:43:59:2D:BA:E8:B3:keyid:20:1D:9C:04:0A:71:B9:E7:8C:28:9D:70:A6:84:43:59:2D:BA:E8:B3", 
                                  "59:0B:DD:26:41:AC:57:D7:ED:76:D5:84:F8:BC:AC:8E:4C:C7:56:70:keyid:59:0B:DD:26:41:AC:57:D7:ED:76:D5:84:F8:BC:AC:8E:4C:C7:56:70", 
                                  provider_connector_dsp_url,
                                 offering_data["odrl:hasPolicy"]["@id"], offering_data["id"],
                                 offering_data["odrl:hasPolicy"], consumer_connector_management_url, edc1_headers)

"""
Check negotiation status
"""
ic("Check negotiation status and wait until finalized")
# Consumer asks own connector for status
agreement_id = poll_negotiation_until_finalized(consumer_connector_management_url, negotiation_id, edc1_headers)

"""
Start data transfer
"""
# Consumer asks own connector to start transfer
ic("Initiate data transfer")
transfer_id = initiate_data_transfer("edc2", provider_connector_dsp_url, agreement_id, asset_id,
                                     {
                                        "type":"IonosS3",
                                        "keyName":"mykey123",
                                        "blobName": None,
                                        "bucketName":"merlotedcconsumer"
                                    },
                                    #create_s3_dataaddress("testfolder/", "merlotedcconsumer", "company2", "testfolder/",
                                    #                       "testfolder/", "s3-eu-central-1.ionoscloud.com"),
                                     consumer_connector_management_url, edc1_headers)

"""
Check transfer status
"""
# Consumer asks own connector for status
ic("Check transfer status and wait until completed")
poll_transfer_until_completed(consumer_connector_management_url, transfer_id, edc1_headers)

# at this point we ask the consumer backend service (separate from the connector) for the authentication token
# and query the public endpoint of the provider with this authorization like a proxy