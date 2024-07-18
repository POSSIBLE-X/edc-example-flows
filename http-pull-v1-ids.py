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
import time

import requests
from icecream import ic
import json

"""
Endpoint configuration
"""
provider_connector_control_url = "http://localhost:19192/control/"
provider_connector_public_url = "http://localhost:19291/public/"
provider_connector_management_url = "http://localhost:19193/api/"
provider_connector_ids_url = "http://localhost:19194/api/"

consumer_connector_control_url = "http://localhost:29192/control/"
consumer_connector_public_url = "http://localhost:29291/public/"
consumer_connector_management_url = "http://localhost:29193/api/"

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
response = requests.post(provider_connector_management_url + "v1/data/instances",
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
response = requests.post(consumer_connector_management_url + "v1/data/instances",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(consumer_dp_instance_data))
ic(response.status_code, response.text)

"""
Create Asset
"""

# Provider
ic("Creating asset in provider connector")
asset_data = {
    "asset": {
        "properties": {
            "asset:prop:id": "assetId",
            "asset:prop:name": "product description",
            "asset:prop:contenttype": "application/json"
        }
    },
    "dataAddress": {
        "properties": {
            "name": "Test asset",
            "baseUrl": "https://jsonplaceholder.typicode.com/users",
            "type": "HttpData"
        }
    }
}

response = requests.post(provider_connector_management_url + "v1/data/assets",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(asset_data))
ic(response.status_code, json.loads(response.text))
# extract asset id from response
asset_id = json.loads(response.text)["id"]

"""
Create Policy
"""

# Provider
ic("Creating policy in provider connector")
policy_data = {
    "id": "aPolicy",
    "policy": {
        "uid": "231802-bb34-11ec-8422-0242ac120002",
        "permissions": [
            {
                "target": "assetId",
                "action": {
                    "type": "USE"
                },
                "edctype": "dataspaceconnector:permission"
            }
        ],
        "@type": {
            "@policytype": "set"
        }
    }
}

response = requests.post(provider_connector_management_url + "v1/data/policydefinitions",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(policy_data))
ic(response.status_code, json.loads(response.text))

"""
Create contract definition
"""

# Provider
ic("Create contract definition for the created asset and policy on provider connector")
contract_definition_data = {
    "id": "1",
    "accessPolicyId": "aPolicy",
    "contractPolicyId": "aPolicy",
    "criteria": []
}

response = requests.post(provider_connector_management_url + "v1/data/contractdefinitions",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(contract_definition_data))
ic(response.status_code, json.loads(response.text))

"""
Fetch catalog from provider
"""

# Consumer asks own connector to query providers catalog
ic("Query providers catalog")
catalog_request_data = {
    "providerUrl": provider_connector_ids_url + "v1/ids/data"
}

response = requests.post(consumer_connector_management_url + "v1/data/catalog/request",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(catalog_request_data))
ic(response.status_code, json.loads(response.text))

offering_data = json.loads(response.text)["contractOffers"][0]

"""
Negotiate contract from available offerings
"""
# Consumer asks own connector to negotiate with providers connector (repeating the offering)
ic("Negotiate offer")
consumer_offer_data = {
    "connectorId": "http-pull-provider",
    "connectorAddress": provider_connector_ids_url + "v1/ids/data",
    "protocol": "ids-multipart",
    "offer": {
        "offerId": offering_data["id"],
        "assetId": offering_data["assetId"],
        "policy": offering_data["policy"]
    }
}

response = requests.post(consumer_connector_management_url + "v1/data/contractnegotiations",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(consumer_offer_data))
ic(response.status_code, json.loads(response.text))

# extract negotiation id
negotiation_id = json.loads(response.text)["id"]

"""
Check negotiation status
"""
ic("Check negotiation status and wait until finalized")
# Consumer asks own connector for status
state = ""

while state != "FINALIZED":
    ic("Requesting status of negotiation")
    response = requests.get(consumer_connector_management_url + "v1/data/contractnegotiations/" + negotiation_id,
                            headers={'Content-Type': 'application/json'})
    state = json.loads(response.text)["state"]
    ic(state)
    time.sleep(1)
ic(response.status_code, json.loads(response.text))
agreement_id = json.loads(response.text)["contractAgreementId"]

"""
Start data transfer
"""
# Consumer asks own connector to start transfer
ic("Initiate data transfer")
transfer_data = {
    "connectorId": "http-pull-provider",
    "connectorAddress": provider_connector_ids_url + "v1/ids/data",
    "contractId": agreement_id,
    "assetId": asset_id,
    "managedResources": "false",
    "dataDestination": {"type": "HttpProxy"}
}
response = requests.post(consumer_connector_management_url + "v1/data/transferprocess",
                         headers={'Content-Type': 'application/json'},
                         data=json.dumps(transfer_data))
ic(response.status_code, json.loads(response.text))
transfer_id = json.loads(response.text)["id"]

"""
Check transfer status
"""
# Consumer asks own connector for status
ic("Check transfer status and wait until completed")
state = ""

while state != "COMPLETED":
    ic("Requesting status of transfer")
    response = requests.get(consumer_connector_management_url + "v1/data/transferprocess/" + transfer_id,
                            headers={'Content-Type': 'application/json'})
    state = json.loads(response.text)["state"]
    ic(state)
    time.sleep(1)
ic(response.status_code, json.loads(response.text))

# at this point we ask the consumer backend service (separate from the connector) for the authentication token
# and query the public endpoint of the provider with this authorization like a proxy