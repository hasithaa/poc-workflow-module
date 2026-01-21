cd native
mvn clean package

cd ../ballerina
rm -rf  ~/.ballerina/repositories/local/bala/hasitha/workflow
rm -rf target
bal clean
bal build
bal pack
bal push --repository=local


# Build Other Examples
cd ../examples/ai_claim_approval/workflow
rm -rf target
bal clean
bal build
