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
from common import create_asset, create_policy, create_contract_definition, deprovision_s3_token, query_catalog, \
    negotiate_offer, poll_negotiation_until_finalized, initiate_data_transfer, poll_transfer_until_completed, \
    create_s3_dataaddress_source, create_s3_dataaddress_destination, edc1_headers, edc2_headers

"""
Endpoint configuration
"""
provider_connector_management_url = "http://localhost:19193/management/"
provider_connector_dsp_url = "http://localhost:19194/protocol"

consumer_connector_management_url = "http://localhost:29193/management/"


"""
Create Asset
"""
# Provider
ic("Creating asset in provider connector")
asset_id = create_asset(str(uuid.uuid4()), "My Asset", "Description", "v1.2.3", "application/json",
                        create_s3_dataaddress_source("s3-eu-central-2.ionoscloud.com", "dev-provider-edc-bucket-possible-31952746", "testfolder/"),
                        provider_connector_management_url, edc2_headers)
ic(asset_id)

"""
Create Policy
"""

# Provider
ic("Creating policy in provider connector")
policy_id = create_policy(str(uuid.uuid4()), provider_connector_management_url, edc2_headers)

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

negotiation_id = negotiate_offer("provider",
                                  "consumer", 
                                  "provider", 
                                  provider_connector_dsp_url,
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
                                     create_s3_dataaddress_destination("s3-eu-central-2.ionoscloud.com", "dev-consumer-edc-bucket-possible-31952746", "myTargetPath/"),
                                     consumer_connector_management_url, edc1_headers)

"""
Check transfer status
"""
# Consumer asks own connector for status
ic("Check transfer status and wait until completed")
poll_transfer_until_completed(consumer_connector_management_url, transfer_id, edc1_headers)

"""
Deprovision S3 token
"""
# Consumer asks own connector for status
ic("Deprovision generated S3 Token")
deprovision_s3_token(consumer_connector_management_url, transfer_id, edc1_headers)