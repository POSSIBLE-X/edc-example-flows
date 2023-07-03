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
                             headers={'Content-Type': 'application/json'},
                             data=json.dumps(provider_dp_instance_data))
    if verbose:
        ic(response.status_code, response.text)


def create_asset(asset_id, asset_name, asset_description, asset_version, asset_contenttype,
                 data_name, data_base_url, data_type,
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
        EDC_PREFIX + "dataAddress": {
            "@type": EDC_PREFIX + "DataAddress",
            EDC_PREFIX + "properties": {
                EDC_PREFIX + "name": data_name,
                EDC_PREFIX + "baseUrl": data_base_url,
                EDC_PREFIX + "type": data_type
            }
        }
    }

    response = requests.post(connector_management_url + "v2/assets",
                             headers={'Content-Type': 'application/json'},
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
                             headers={'Content-Type': 'application/json'},
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
                             headers={'Content-Type': 'application/json'},
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
                             headers={'Content-Type': 'application/json'},
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
                             headers={'Content-Type': 'application/json'},
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
                                headers={'Content-Type': 'application/json'})
        state = json.loads(response.text)[EDC_PREFIX + "state"]
        if verbose:
            ic(state)
        time.sleep(1)
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)[EDC_PREFIX + "contractAgreementId"]


def initiate_data_transfer(connector_id, connector_address, agreement_id, asset_id, data_destination_properties,
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
        EDC_PREFIX + "dataDestination": {
            "@type": EDC_PREFIX + "DataAddress",
            EDC_PREFIX + "properties": data_destination_properties
        }
    }
    response = requests.post(connector_management_url + "v2/transferprocesses",
                             headers={'Content-Type': 'application/json'},
                             data=json.dumps(transfer_data))
    if verbose:
        ic(response.status_code, json.loads(response.text))
    return json.loads(response.text)["@id"]


def poll_transfer_until_completed(connector_management_url, transfer_id, verbose=True):
    state = ""

    while state != "COMPLETED":
        ic("Requesting status of transfer")
        response = requests.get(connector_management_url + "v2/transferprocesses/" + transfer_id,
                                headers={'Content-Type': 'application/json'})
        state = json.loads(response.text)[EDC_PREFIX + "state"]
        if verbose:
            ic(state)
        time.sleep(1)

    if verbose:
        ic(response.status_code, json.loads(response.text))