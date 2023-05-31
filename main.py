import time

import requests
from icecream import ic
import json

"""
Endpoint configuration
"""
provider_connector_control_url = "http://localhost:19192/control/"
provider_connector_public_url = "http://localhost:19291/public/"
provider_connector_management_url = "http://localhost:19193/api/v1/data/"
provider_connector_dsp_url = "http://localhost:19194/api/v1/dsp"

consumer_connector_control_url = "http://localhost:29192/control/"
consumer_connector_public_url = "http://localhost:29291/public/"
consumer_connector_management_url = "http://localhost:29193/api/v1/data/"
consumer_connector_dsp_url = "http://localhost:29194/api/v1/dsp"

"""
Constants
"""

CONTEXT = {
    "@vocab": "https://w3id.org/edc/v0.0.1/ns/",
    "edc": "https://w3id.org/edc/v0.0.1/ns/",
    "odrl": "http://www.w3.org/ns/odrl/2/",
},

EDC_PREFIX = "edc:"
ODRL_PREFIX = "odrl:"

"""
Connector initialization
"""

# Provider
ic("Preparing provider connector dataplane")
provider_dp_instance_data = {
    "edctype": "dataspaceconnector:dataplaneinstance",
    "id": "http-pull-provider-dataplane",
    "url": provider_connector_control_url + "transfer",
    "allowedSourceTypes": ["HttpData"],
    "allowedDestTypes": ["HttpProxy", "HttpData"],
    "properties": {
        "publicApiUrl": provider_connector_public_url
    }
}
response = requests.post(provider_connector_management_url + "instances",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(provider_dp_instance_data))
ic(response.status_code, response.text)

# Consumer
ic("Preparing consumer connector dataplane")
consumer_dp_instance_data = {
    "edctype": "dataspaceconnector:dataplaneinstance",
    "id": "http-pull-consumer-dataplane",
    "url": consumer_connector_control_url + "transfer",
    "allowedSourceTypes": ["HttpData"],
    "allowedDestTypes": ["HttpProxy", "HttpData"],
    "properties": {
        "publicApiUrl": consumer_connector_public_url
    }
}
response = requests.post(consumer_connector_management_url + "instances",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(consumer_dp_instance_data))
ic(response.status_code, response.text)

"""
Create Asset
"""

# Provider
ic("Creating asset in provider connector")
asset_data = {
    "@context": CONTEXT,
    EDC_PREFIX + "asset": {
        "@type": EDC_PREFIX + "Asset",
        "@id": "test-asset-id",
        EDC_PREFIX + "properties": {
            EDC_PREFIX + "id": "test-asset-id",
            EDC_PREFIX + "name": "assetId",
            EDC_PREFIX + "description": "product description",
            EDC_PREFIX + "version": "0.4.2",
            EDC_PREFIX + "contenttype": "application/json"
        }
    },
    EDC_PREFIX + "dataAddress": {
        "@type": EDC_PREFIX + "DataAddress",
        EDC_PREFIX + "properties": {
            EDC_PREFIX + "name": "Test asset",
            EDC_PREFIX + "baseUrl": "https://jsonplaceholder.typicode.com/users",
            EDC_PREFIX + "type": "HttpData"
        }
    }
}

response = requests.post(provider_connector_management_url + "v2/assets",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(asset_data))
ic(response.status_code, json.loads(response.text))
# extract asset id from response
asset_id = json.loads(response.text)["@id"]

"""
Create Policy
"""

# Provider
ic("Creating policy in provider connector")
policy_data = {
    "@context": CONTEXT,
    "@id": "aPolicy",
    EDC_PREFIX + "policy": {
        ODRL_PREFIX + "permission": [
            {
                ODRL_PREFIX + "target": "assetId",
                ODRL_PREFIX + "action": {
                    ODRL_PREFIX + "type": "USE"
                },
                ODRL_PREFIX + "edctype": "dataspaceconnector:permission"
            }
        ],
        "@type": ODRL_PREFIX + "Set"
    }
}

response = requests.post(provider_connector_management_url + "v2/policydefinitions",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(policy_data))
ic(response.status_code, json.loads(response.text))

"""
Create contract definition
"""

# Provider
ic("Create contract definition for the created asset and policy on provider connector")
contract_definition_data = {
    "@context": CONTEXT,
    "@type": EDC_PREFIX + "ContractDefinition",
    "@id": "11",
    EDC_PREFIX + "accessPolicyId": "aPolicy",
    EDC_PREFIX + "contractPolicyId": "aPolicy",
    EDC_PREFIX + "criteria": []
}

response = requests.post(provider_connector_management_url + "v2/contractdefinitions",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(contract_definition_data))
ic(response.status_code, json.loads(response.text))

"""
Fetch catalog from provider
"""

# Consumer asks own connector to query providers catalog
ic("Query providers catalog")
catalog_request_data = {
    "@context": CONTEXT,
    EDC_PREFIX + "providerUrl": provider_connector_dsp_url,
    EDC_PREFIX + "protocol": "dataspace-protocol-http"
}

response = requests.post(consumer_connector_management_url + "v2/catalog/request",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(catalog_request_data))
ic(response.status_code, json.loads(response.text))

offering_data = json.loads(response.text)["dcat:dataset"]
if not isinstance(offering_data, dict):  # if there are multiple entries, choose the first one
    offering_data = offering_data[0]

"""
Negotiate contract from available offerings
"""
# Consumer asks own connector to negotiate with providers connector (repeating the offering)
ic("Negotiate offer")
consumer_offer_data = {
    "@context": CONTEXT,
    "@type": EDC_PREFIX + "NegotiationInitiateRequestDto",
    EDC_PREFIX + "connectorId": "provider",
    EDC_PREFIX + "connectorAddress": provider_connector_dsp_url,
    EDC_PREFIX + "protocol": "dataspace-protocol-http",
    EDC_PREFIX + "offer": {
        "@type": EDC_PREFIX + "ContractOfferDescription",
        EDC_PREFIX + "offerId": offering_data["odrl:hasPolicy"]["@id"],
        EDC_PREFIX + "assetId": offering_data["edc:id"],
        EDC_PREFIX + "policy": offering_data["odrl:hasPolicy"]
    }
}

response = requests.post(consumer_connector_management_url + "v2/contractnegotiations",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(consumer_offer_data))
ic(response.status_code, response.text)

# extract negotiation id
negotiation_id = json.loads(response.text)["@id"]

"""
Check negotiation status
"""
ic("Check negotiation status and wait until finalized")
# Consumer asks own connector for status
state = ""

while state != "FINALIZED":
    ic("Requesting status of negotiation")
    response = requests.get(consumer_connector_management_url + "v2/contractnegotiations/" + negotiation_id,
                            headers={'Content-Type': 'application/json'})
    state = json.loads(response.text)[EDC_PREFIX + "state"]
    ic(state)
    time.sleep(1)
ic(response.status_code, json.loads(response.text))
agreement_id = json.loads(response.text)[EDC_PREFIX + "contractAgreementId"]

"""
Start data transfer
"""
# Consumer asks own connector to start transfer
ic("Initiate data transfer")
transfer_data = {
    "@context": CONTEXT,
    "@type": EDC_PREFIX + "TransferRequestDto",
    EDC_PREFIX + "connectorId": "provider",
    EDC_PREFIX + "connectorAddress": provider_connector_dsp_url,
    EDC_PREFIX + "contractId": agreement_id,
    EDC_PREFIX + "assetId": asset_id,
    EDC_PREFIX + "managedResources": False,
    EDC_PREFIX + "protocol": "dataspace-protocol-http",
    EDC_PREFIX + "dataDestination": {
        "@type": EDC_PREFIX + "DataAddress",
        EDC_PREFIX + "properties": {
            EDC_PREFIX + "type": "HttpProxy"
        }
    }
}
response = requests.post(consumer_connector_management_url + "v2/transferprocesses",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(transfer_data))
ic(response.status_code, json.loads(response.text))
transfer_id = json.loads(response.text)["@id"]

"""
Check transfer status
"""
# Consumer asks own connector for status
ic("Check transfer status and wait until completed")
state = ""

while state != "COMPLETED":
    ic("Requesting status of transfer")
    response = requests.get(consumer_connector_management_url + "v2/transferprocesses/" + transfer_id,
                            headers={'Content-Type': 'application/json'})
    state = json.loads(response.text)[EDC_PREFIX + "state"]
    ic(state)
    time.sleep(1)

ic(response.status_code, json.loads(response.text))

# at this point we ask the consumer backend service (separate from the connector) for the authentication token
# and query the public endpoint of the provider with this authorization like a proxy
