from icecream import ic
from common_v2 import create_asset, create_policy, create_contract_definition, query_catalog, \
    negotiate_offer, poll_negotiation_until_finalized, initiate_data_transfer, poll_transfer_until_completed, \
    create_s3_dataaddress

"""
Endpoint configuration
"""
provider_connector_management_url = "http://localhost:8182/management/"
provider_connector_dsp_url = "http://localhost:8282/protocol"

consumer_connector_management_url = "http://localhost:9192/management/"
consumer_connector_dsp_url = "http://localhost:9292/protocol"

"""
Create Asset
"""

# Provider
ic("Creating asset in provider connector")
asset_id = create_asset("assetId", "My Asset", "Description", "v1.2.3", "application/json",
                        create_s3_dataaddress("device1-data.csv", "merlotedcprovider", "company1", "device1-data.csv",
                                              "device1-data.csv", "s3-eu-central-1.ionoscloud.com"),
                        provider_connector_management_url)

"""
Create Policy
"""

# Provider
ic("Creating policy in provider connector")
policy_id = create_policy("aPolicy", "assetId", provider_connector_management_url)

"""
Create contract definition
"""

# Provider
ic("Create contract definition for the created asset and policy on provider connector")
create_contract_definition(policy_id, policy_id, "assetId", provider_connector_management_url)

"""
Fetch catalog from provider
"""

# Consumer asks own connector to query providers catalog
ic("Query providers catalog")

offering_data = query_catalog(provider_connector_dsp_url, consumer_connector_management_url)
if not isinstance(offering_data, dict):  # if there are multiple entries, choose the first one
    offering_data = offering_data[0]

"""
Negotiate contract from available offerings
"""
# Consumer asks own connector to negotiate with providers connector (repeating the offering)
ic("Negotiate offer")

negotiation_id = negotiate_offer("provider", "consumer", "provider", provider_connector_dsp_url,
                                 offering_data["odrl:hasPolicy"]["@id"], offering_data["edc:id"],
                                 offering_data["odrl:hasPolicy"], consumer_connector_management_url)

"""
Check negotiation status
"""
ic("Check negotiation status and wait until finalized")
# Consumer asks own connector for status
agreement_id = poll_negotiation_until_finalized(consumer_connector_management_url, negotiation_id)

"""
Start data transfer
"""
# Consumer asks own connector to start transfer
ic("Initiate data transfer")
transfer_id = initiate_data_transfer("provider", provider_connector_dsp_url, agreement_id, asset_id,
                                     create_s3_dataaddress("device1-data.csv", "merlotedcconsumer", "company2", "device1-data.csv",
                                                           "device1-data.csv", "s3-eu-central-1.ionoscloud.com"),
                                     consumer_connector_management_url)

"""
Check transfer status
"""
# Consumer asks own connector for status
ic("Check transfer status and wait until completed")
poll_transfer_until_completed(consumer_connector_management_url, transfer_id)

# at this point we ask the consumer backend service (separate from the connector) for the authentication token
# and query the public endpoint of the provider with this authorization like a proxy