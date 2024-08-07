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
from common import create_dataplane, create_asset, create_policy, create_contract_definition, query_catalog, \
    negotiate_offer, poll_negotiation_until_finalized, initiate_data_transfer, poll_transfer_until_completed, \
    create_http_dataaddress, create_http_proxy_dataaddress, edc1_headers, edc2_headers

"""
Endpoint configuration
"""
provider_connector_control_url = "http://localhost:19192/control/"
provider_connector_public_url = "http://localhost:19291/public/"
provider_connector_management_url = "http://localhost:19193/management/"
provider_connector_dsp_url = "http://localhost:19194/protocol"

consumer_connector_control_url = "http://localhost:29192/control/"
consumer_connector_public_url = "http://localhost:29291/public/"
consumer_connector_management_url = "http://localhost:29193/management/"

"""
Connector initialization
"""

# Provider
ic("Preparing provider connector dataplane")
create_dataplane(provider_connector_control_url + "transfer", provider_connector_public_url,
                 provider_connector_management_url)

# Consumer
ic("Preparing consumer connector dataplane")
create_dataplane(consumer_connector_control_url + "transfer", consumer_connector_public_url,
                 consumer_connector_management_url)

"""
Create Asset
"""

# Provider
ic("Creating asset in provider connector")
asset_id = create_asset("assetId", "My Asset", "Description", "v1.2.3", "application/json",
                        create_http_dataaddress("My Asset", "https://jsonplaceholder.typicode.com/users"),
                        provider_connector_management_url, edc2_headers)

"""
Create Policy
"""

# Provider
ic("Creating policy in provider connector")
policy_id = create_policy("aPolicy", provider_connector_management_url, edc2_headers)

"""
Create contract definition
"""

# Provider
ic("Create contract definition for the created asset and policy on provider connector")
create_contract_definition(policy_id, policy_id, provider_connector_management_url, edc2_headers)

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

# modify offer
# offering_data["odrl:hasPolicy"]["odrl:permission"]["odrl:action"]["odrl:type"] = "DISTRIBUTE"
# this fails immediately in ContractValidationServiceImpl.java:validateConfirmed(...) which checks for equality.

ic("Negotiate offer")

negotiation_id = negotiate_offer("provider", "consumer", "provider", provider_connector_dsp_url,
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
transfer_id = initiate_data_transfer("provider", provider_connector_dsp_url, agreement_id, asset_id,
                                     create_http_proxy_dataaddress(),
                                     consumer_connector_management_url, edc1_headers)

"""
Check transfer status
"""
# Consumer asks own connector for status
ic("Check transfer status and wait until completed")
poll_transfer_until_completed(consumer_connector_management_url, transfer_id, edc1_headers)

# at this point we ask the consumer backend service (separate from the connector) for the authentication token
# and query the public endpoint of the provider with this authorization like a proxy
