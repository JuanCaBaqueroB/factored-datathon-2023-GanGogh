import urllib.request
import json
import os
import ssl
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

key_vault_name = "mlfactoreddata3978247496"
keyvault_uri = f"https://{key_vault_name}.vault.azure.net"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=keyvault_uri, credential=credential)


def allowSelfSignedHttps(allowed):
    # bypass the server certificate verification on client side
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '')\
        and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True)
# this line is needed if you use self-signed certificate in your scoring service

# Request data goes here
# The example below assumes JSON formatting which may be updated
# depending on the format your endpoint expects.


def run_summary_model(some_review: str) -> str:
    classification_prompt = f"Classsify the following review as either "\
                            + f"Product Quality Problem, Shipping Problem, "\
                            + f"Disliked Product or High Price: {some_review}"
    summarization_prompt = f"Summarize the issue in the following product "\
                            + f"review: {some_review}"
    data = {"inputs": [classification_prompt, summarization_prompt]}

    body = str.encode(json.dumps(data))

    url = 'https://ml-factored-datathon-sum.eastus.inference.ml.azure.com/score'
    # Replace this with the primary/secondary key or AMLToken for the endpoint
    api_key = client.get_secret("SUMMARY-API-KEY")
    if not api_key:
        raise Exception("A key should be provided to invoke the endpoint")

    # The azureml-model-deployment header will force the request to go to a
    # specific deployment.
    # Remove this header to have the request observe the endpoint traffic rules
    headers = {'Content-Type':'application/json',
               'Authorization':('Bearer '+ api_key),
               'azureml-model-deployment': 'google-flan-t5-large-13'
               }

    print('launching request')
    req = urllib.request.Request(url, body, headers)

    try:
        response = urllib.request.urlopen(req)
        result = response.read()
        print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp,
        # which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))
    return result


some_review = """I ordered 3 pairs. Only one lasted more that a couple of"""\
              + """ weeks. I even ordered on size larger in case the fit"""\
              + """ wasn't right. The larger pair ripped in the crotch"""\
              + """ within 3 wearings. The other one, the button broke,"""\
              + """ (not just fell off) shattered while I was giving a """\
              + """lecture. (Very embarrassing!) I won't wear the last """\
              + """pair because I don't trust them. None of the pants """\
              + """were tight, they were actually very comfortable. """\
              + """I guess they just don't stand up to the rigorous """\
              + """life of a junior high school teacher. I don't """\
              + """usually take the time to write reviews but I live """\
              + """in Japan, clothes that fit are extremely hard to come by."""\
              + """ Losing those pants was a huge blow to my wallet."""
run_summary_model(some_review)
