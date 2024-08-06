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
import uuid


CONTEXT = {
    "@vocab": "https://w3id.org/edc/v0.0.1/ns/",
    "edc": "https://w3id.org/edc/v0.0.1/ns/",
    "odrl": "http://www.w3.org/ns/odrl/2/"
},

edc1_headers = {
    'Content-Type': 'application/json',
    'X-API-Key': '1234'
}

edc2_headers = {
    'Content-Type': 'application/json',
    'X-API-Key': '5678'
}


def create_dataplane(transfer_url, public_api_url, connector_management_url, edc_headers, verbose=True):
    provider_dp_instance_data = {
        "edctype": "dataspaceconnector:dataplaneinstance",
        "id": "http-pull-provider-dataplane",
        "url": transfer_url,
        "allowedSourceTypes": ["HttpData"],
        "allowedDestTypes": ["HttpProxy", "HttpData"],
        "properties": {
            "publicApiUrl": public_api_url
        }
    }
    ic(provider_dp_instance_data)
    response = requests.post(connector_management_url + "instances",
                             headers=edc_headers,
                             data=json.dumps(provider_dp_instance_data))
    if verbose:
        ic(response.status_code, response.text)


def create_http_dataaddress(name, base_url):
    return {
            "type": "HttpData",
            "name": name,
            "baseUrl": base_url,
            "proxyPath": "true"
        }


def create_http_proxy_dataaddress():
    return {
            "type": "HttpProxy"
        }


def create_s3_dataaddress_source(storage, bucket_name, blob_name, name="someName", container="someContainer"):
    return {
        "bucketName": bucket_name,
        "blobName": blob_name,
        "storage": storage,
        "type": "IonosS3",
        # the following parameters seem to have no effect
        "name": name,
        "container": container,
    }

def create_s3_dataaddress_destination(storage, bucket_name, dest_path, key_name="someKey"):
    return {
        "type": "IonosS3",
        "storage": storage,
        "bucketName": bucket_name,
        "path": dest_path,
        # the following parameters seem to have no effect
        "keyName": key_name
    }


def create_asset(asset_id, asset_name, asset_description, asset_version, asset_contenttype, data_address,
                 connector_management_url, edc_headers, verbose=True):
    asset_data = {
        "@context": CONTEXT,
        "@id": asset_id,
        "properties": {
            "name": asset_name,
            "contenttype": asset_contenttype,
            "description": asset_description,
            "version": asset_version
        },
        "dataAddress": data_address
    }
    ic(asset_data)

    response = requests.post(connector_management_url + "v3/assets",
                             headers=edc_headers,
                             data=json.dumps(asset_data))
    if verbose:
        ic(response.status_code)
        ic(json.loads(response.text))
    # extract asset id from response
    return json.loads(response.text)["@id"]


def create_policy(policy_id, connector_management_url, edc_headers, verbose=True):
    policy_data = {
        "@context": CONTEXT,
        "@id": policy_id,
        "policy": {
            "@type": "set",
            "odrl:permission": [],
            "odrl:prohibition": [],
            "odrl:obligation": []
        }
    }

    ic(policy_data)

    response = requests.post(connector_management_url + "v2/policydefinitions",
                             headers=edc_headers,
                             data=json.dumps(policy_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)["@id"]


def create_contract_definition(access_policy_id, contract_policy_id, asset_id, connector_management_url, edc_headers, verbose=True):
    contract_definition_data = {
        "@context": CONTEXT,
        "@id": str(uuid.uuid4()),
        "accessPolicyId": access_policy_id,
        "contractPolicyId": contract_policy_id,
        "assetsSelector": [
            {
                "operandLeft": "https://w3id.org/edc/v0.0.1/ns/id",
                "operator": "=",
                "operandRight": asset_id
            }
        ]
    }

    ic(contract_definition_data)

    response = requests.post(connector_management_url + "v2/contractdefinitions",
                             headers=edc_headers,
                             data=json.dumps(contract_definition_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))


def query_catalog(provider_url, connector_management_url, edc_headers, verbose=True):
    catalog_request_data = {
        "@context": CONTEXT,
        "counterPartyAddress": provider_url,
        "protocol": "dataspace-protocol-http"
    }

    ic(catalog_request_data)

    response = requests.post(connector_management_url + "v2/catalog/request",
                             headers=edc_headers,
                             data=json.dumps(catalog_request_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))

    return json.loads(response.text)["dcat:dataset"]


def negotiate_offer(connector_id, consumer_id, provider_id, connector_address, offer_id, asset_id, policy,
                    connector_management_url, edc_headers, verbose=True):
    consumer_offer_data = {
        "@context": CONTEXT,
        "@type": "NegotiationInitiateRequestDto",
        "connectorId": connector_id,
        "consumerId": consumer_id,
        "providerId": provider_id,
        "counterPartyAddress": connector_address,
        "protocol": "dataspace-protocol-http",
        "policy": {
            "@context": "http://www.w3.org/ns/odrl.jsonld",
            "@type": "Set",
            "@id": offer_id,
            "permission": [],
            "prohibition": [],
            "obligation": [],
            "target": asset_id
        }
    }

    ic(consumer_offer_data)

    response = requests.post(connector_management_url + "v2/contractnegotiations",
                             headers=edc_headers,
                             data=json.dumps(consumer_offer_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))

    # extract negotiation id
    return json.loads(response.text)["@id"]


def poll_negotiation_until_finalized(connector_management_url, negotiation_id, edc_headers, verbose=True):
    state = ""

    while state != "FINALIZED":
        ic("Requesting status of negotiation")
        response = requests.get(connector_management_url + "v2/contractnegotiations/" + negotiation_id,
                                headers=edc_headers)
        state = json.loads(response.text)["state"]
        if verbose:
            ic(state)
        time.sleep(1)
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)["contractAgreementId"]


def initiate_data_transfer(connector_id, connector_address, agreement_id, asset_id, data_destination,
                           connector_management_url, edc_headers, verbose=True):
    transfer_data = {
        "@context": CONTEXT,
        "@type": "TransferRequestDto",
        "connectorId": connector_id,
        "counterPartyAddress": connector_address,
        "contractId": agreement_id,
        "assetId": asset_id,  # seems to be unused by the EDC currently
        "protocol": "dataspace-protocol-http",
        "dataDestination": data_destination
    }

    ic(transfer_data)

    response = requests.post(connector_management_url + "v2/transferprocesses",
                             headers=edc_headers,
                             data=json.dumps(transfer_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)["@id"]


def poll_transfer_until_completed(connector_management_url, transfer_id, edc_headers, verbose=True):
    state = ""

    while state != "COMPLETED":
        ic("Requesting status of transfer")
        response = requests.get(connector_management_url + "v2/transferprocesses/" + transfer_id,
                                headers=edc_headers)
        state = json.loads(response.text)["state"]
        if verbose:
            ic(state)
        time.sleep(1)

    if verbose:
        ic(response.status_code, json.loads(response.text))

def deprovision_s3_token(connector_management_url, transfer_id, edc_headers, verbose=True):
    ic("Requesting status of transfer")
    response = requests.post(connector_management_url + "/v2/transferprocesses/" + transfer_id + "/deprovision",
                            headers=edc_headers)
    if verbose:
        ic(response.status_code, json.loads(response.text))