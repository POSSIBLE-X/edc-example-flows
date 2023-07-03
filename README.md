# Example flows for interaction with EDC Connectors
This repository contains a range of example flows written in python to get a better understanding how to interact with EDC Connectors.
It is derived and extended from the examples in https://github.com/eclipse-edc/Samples.

The v1 scripts are based on version v0.0.1-milestone8 of the EDC while the v2 scripts are based on the more recent v0.1.0 of the EDC.

## HTTP Pull v2
- Build the Connectors and consumer backend:
``` 
cd connector/v2
./gradlew transfer:transfer-06-consumer-pull-http:http-pull-connector:build 
./gradlew transfer:transfer-06-consumer-pull-http:consumer-pull-backend-service:build
```
- Run the Connectors and consumer backend service:
``` 
# create 3 terminals, each running one of the following commands
# terminal 1
java -Dedc.keystore=transfer/transfer-06-consumer-pull-http/certs/cert.pfx -Dedc.keystore.password=123456 -Dedc.vault=transfer/transfer-06-consumer-pull-http/http-pull-consumer/consumer-vault.properties -Dedc.fs.config=transfer/transfer-06-consumer-pull-http/http-pull-consumer/consumer-configuration.properties -jar transfer/transfer-06-consumer-pull-http/http-pull-connector/build/libs/pull-connector.jar

# terminal 2
java -Dedc.keystore=transfer/transfer-06-consumer-pull-http/certs/cert.pfx -Dedc.keystore.password=123456 -Dedc.vault=transfer/transfer-06-consumer-pull-http/http-pull-provider/provider-vault.properties -Dedc.fs.config=transfer/transfer-06-consumer-pull-http/http-pull-provider/provider-configuration.properties -jar transfer/transfer-06-consumer-pull-http/http-pull-connector/build/libs/pull-connector.jar

# terminal 3
java -jar transfer/transfer-06-consumer-pull-http/consumer-pull-backend-service/build/libs/consumer-pull-backend-service.jar
``` 
- Execute example flow and watch the outputs of all terminals:
``` 
# cd back to root of repo
python3 http-pull-v2-dsp.py
``` 

After running these commands, you should see some output on the consumer backend terminal, which contains an authentication token and a url where the consumer can get the data from using
```
curl --location --request GET '<url>' \
--header 'Authorization: <auth code>'
```
Since the consumer has to manually request the data, this is a pull flow.


## HTTP Push v2
- Build the Connectors and consumer backend:
``` 
cd connector/v2
./gradlew transfer:transfer-07-provider-push-http:http-push-connector:build
./gradlew transfer:transfer-07-provider-push-http:provider-push-http-backend-service:build
```
- Run the Connectors and consumer backend service:
``` 
# create 3 terminals, each running one of the following commands
# terminal 1
java -Dedc.keystore=transfer/transfer-07-provider-push-http/certs/cert.pfx -Dedc.keystore.password=123456 -Dedc.vault=transfer/transfer-07-provider-push-http/http-push-provider/provider-vault.properties -Dedc.fs.conf
ig=transfer/transfer-07-provider-push-http/http-push-provider/provider-configuration.properties -jar transfer/transfer-07-provider-push-http/http-push-connector/build/libs/push-connector.jar

# terminal 2
java -Dedc.keystore=transfer/transfer-07-provider-push-http/certs/cert.pfx -Dedc.keystore.password=123456 -Dedc.vault=transfer/transfer-07-provider-push-http/http-push-consumer/consumer-vault.properties -Dedc.fs.conf
ig=transfer/transfer-07-provider-push-http/http-push-consumer/consumer-configuration.properties -jar transfer/transfer-07-provider-push-http/http-push-connector/build/libs/push-connector.jar

# terminal 3
java -jar transfer/transfer-07-provider-push-http/provider-push-http-backend-service/build/libs/provider-push-http-backend-service.jar
``` 
- Execute example flow and watch the outputs of all terminals:
``` 
# cd back to root of repo
python3 http-push-v2-dsp.py
``` 
After running these commands, you should see some output on the consumer backend terminal, which contains the data from the provider side (which in our example is a publicly hosted demo-json).

## IONOS S3 Push
- Configure connectors:
  - adjust edc.ionos.access.key and edc.ionos.secret.key in example/file-transfer-push/consumer/resources/consumer-config.properties
  - adjust edc.ionos.access.key and edc.ionos.secret.key in example/file-transfer-push/provider/resources/provider-config.properties
- Build the Connectors and consumer backend:
``` 
cd connector/v2
./gradlew :example:file-transfer-push:provider:build
./gradlew :example:file-transfer-push:consumer:build
./gradlew :example:file-transfer-push:transfer-file:build

```
- Run the Connectors and consumer backend service:
``` 
# create 2 terminals, each running one of the following commands
# terminal 1
java -Dedc.fs.config=example/file-transfer-push/consumer/resources/consumer-config.properties -jar example/file-transfer-push/consumer/build/libs/dataspace-connector.jar

# terminal 2
java -Dedc.fs.config=example/file-transfer-push/provider/resources/provider-config.properties -jar example/file-transfer-push/provider/build/libs/dataspace-connector.jar
``` 
- Execute example flow and watch the outputs of all terminals:
``` 
# cd back to root of repo
python3 s3-push.py
``` 
After running these commands, a csv file should have been transferred from the provider bucket to the consumer bucket.