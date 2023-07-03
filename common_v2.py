import time

import requests
from icecream import ic
import json


CONTEXT = {
    "@vocab": "https://w3id.org/edc/v0.0.1/ns/",
    "edc": "https://w3id.org/edc/v0.0.1/ns/",
    "odrl": "http://www.w3.org/ns/odrl/2/",
},

EDC_PREFIX = "edc:"
ODRL_PREFIX = "odrl:"

edc_headers = {
    'Content-Type': 'application/json',
    'X-API-Key': 'password'
}


def create_dataplane(transfer_url, public_api_url, connector_management_url, verbose=True):
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
    response = requests.post(connector_management_url + "instances",
                             headers=edc_headers,
                             data=json.dumps(provider_dp_instance_data))
    if verbose:
        ic(response.status_code, response.text)


def create_http_dataaddress(name, base_url):
    return {
            "@type": EDC_PREFIX + "DataAddress",
            EDC_PREFIX + "properties": {
                EDC_PREFIX + "name": name,
                EDC_PREFIX + "baseUrl": base_url,
                EDC_PREFIX + "type": "HttpData"
            }
        }


def create_http_proxy_dataaddress():
    return {
            "@type": EDC_PREFIX + "DataAddress",
            EDC_PREFIX + "properties": {
                EDC_PREFIX + "type": "HttpProxy"
            }
        }


def create_s3_dataaddress(name, bucket_name, container, blob_name, key_name, storage):
    return {
            "@type": EDC_PREFIX + "DataAddress",
            EDC_PREFIX + "name": name,
            EDC_PREFIX + "bucketName": bucket_name,
            EDC_PREFIX + "container": container,
            EDC_PREFIX + "blobName": blob_name,
            EDC_PREFIX + "keyName": key_name,
            EDC_PREFIX + "storage": storage,
            EDC_PREFIX + "type": "IonosS3"
        }


def create_asset(asset_id, asset_name, asset_description, asset_version, asset_contenttype, data_address,
                 connector_management_url, verbose=True):
    asset_data = {
        "@context": CONTEXT,
        EDC_PREFIX + "asset": {
            "@type": EDC_PREFIX + "Asset",
            "@id": asset_id,
            EDC_PREFIX + "properties": {
                EDC_PREFIX + "name": asset_name,
                EDC_PREFIX + "description": asset_description,
                EDC_PREFIX + "version": asset_version,
                EDC_PREFIX + "contenttype": asset_contenttype
            }
        },
        EDC_PREFIX + "dataAddress": data_address
    }
    ic(asset_data)

    response = requests.post(connector_management_url + "v2/assets",
                             headers=edc_headers,
                             data=json.dumps(asset_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))
    # extract asset id from response
    return json.loads(response.text)["@id"]


def create_policy(policy_id, target_asset_id, connector_management_url, verbose=True):  # TODO for now we always use the same permissions
    policy_data = {
        "@context": CONTEXT,
        "@id": policy_id,
        EDC_PREFIX + "policy": {
            "@context": "http://www.w3.org/ns/odrl.jsonld",
            ODRL_PREFIX + "permission": [
                {
                    ODRL_PREFIX + "target": target_asset_id,
                    ODRL_PREFIX + "action": {
                        ODRL_PREFIX + "type": "USE"
                    },
                    ODRL_PREFIX + "edctype": "dataspaceconnector:permission"
                }
            ],
            "@type": ODRL_PREFIX + "Set"
        }
    }

    response = requests.post(connector_management_url + "v2/policydefinitions",
                             headers=edc_headers,
                             data=json.dumps(policy_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)["@id"]


def create_contract_definition(access_policy_id, contract_policy_id, asset_id, connector_management_url, verbose=True):  # TODO for now we use no selector (i.e. all assets are selected)
    contract_definition_data = {
        "@context": CONTEXT,
        "@type": EDC_PREFIX + "ContractDefinition",
        "@id": "1",
        EDC_PREFIX + "accessPolicyId": access_policy_id,
        EDC_PREFIX + "contractPolicyId": contract_policy_id,
        EDC_PREFIX + "assetsSelector": [
        ]
    }

    response = requests.post(connector_management_url + "v2/contractdefinitions",
                             headers=edc_headers,
                             data=json.dumps(contract_definition_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))


def query_catalog(provider_url, connector_management_url, verbose=True):
    catalog_request_data = {
        "@context": CONTEXT,
        EDC_PREFIX + "providerUrl": provider_url,
        EDC_PREFIX + "protocol": "dataspace-protocol-http"
    }

    response = requests.post(connector_management_url + "v2/catalog/request",
                             headers=edc_headers,
                             data=json.dumps(catalog_request_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))

    return json.loads(response.text)["dcat:dataset"]


def negotiate_offer(connector_id, consumer_id, provider_id, connector_address, offer_id, asset_id, policy,
                    connector_management_url, verbose=True):
    consumer_offer_data = {
        "@context": CONTEXT,
        "@type": EDC_PREFIX + "NegotiationInitiateRequestDto",
        EDC_PREFIX + "connectorId": connector_id,
        EDC_PREFIX + "consumerId": consumer_id,
        EDC_PREFIX + "providerId": provider_id,
        EDC_PREFIX + "connectorAddress": connector_address,
        EDC_PREFIX + "protocol": "dataspace-protocol-http",
        EDC_PREFIX + "offer": {
            "@type": EDC_PREFIX + "ContractOfferDescription",
            EDC_PREFIX + "offerId": offer_id,
            EDC_PREFIX + "assetId": asset_id,
            EDC_PREFIX + "policy": policy
        }
    }

    response = requests.post(connector_management_url + "v2/contractnegotiations",
                             headers=edc_headers,
                             data=json.dumps(consumer_offer_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))

    # extract negotiation id
    return json.loads(response.text)["@id"]


def poll_negotiation_until_finalized(connector_management_url, negotiation_id, verbose=True):
    state = ""

    while state != "FINALIZED":
        ic("Requesting status of negotiation")
        response = requests.get(connector_management_url + "v2/contractnegotiations/" + negotiation_id,
                                headers=edc_headers)
        state = json.loads(response.text)[EDC_PREFIX + "state"]
        if verbose:
            ic(state)
        time.sleep(1)
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)[EDC_PREFIX + "contractAgreementId"]


def initiate_data_transfer(connector_id, connector_address, agreement_id, asset_id, data_destination,
                           connector_management_url, verbose=True):
    transfer_data = {
        "@context": CONTEXT,
        "@type": EDC_PREFIX + "TransferRequestDto",
        EDC_PREFIX + "connectorId": connector_id,
        EDC_PREFIX + "connectorAddress": connector_address,
        EDC_PREFIX + "contractId": agreement_id,
        EDC_PREFIX + "assetId": asset_id,
        EDC_PREFIX + "managedResources": False,
        EDC_PREFIX + "protocol": "dataspace-protocol-http",
        EDC_PREFIX + "dataDestination": data_destination
    }
    response = requests.post(connector_management_url + "v2/transferprocesses",
                             headers=edc_headers,
                             data=json.dumps(transfer_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)["@id"]


def poll_transfer_until_completed(connector_management_url, transfer_id, verbose=True):
    state = ""

    while state != "COMPLETED":
        ic("Requesting status of transfer")
        response = requests.get(connector_management_url + "v2/transferprocesses/" + transfer_id,
                                headers=edc_headers)
        state = json.loads(response.text)[EDC_PREFIX + "state"]
        if verbose:
            ic(state)
        time.sleep(1)

    if verbose:
        ic(response.status_code, json.loads(response.text))