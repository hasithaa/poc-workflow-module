cd native
mvn clean package

cd ../ballerina
rm -rf  ~/.ballerina/repositories/local/bala/hasitha/workflow
rm -rf target
bal clean
bal build
bal pack
bal push --repository=local

# If example changed or dependencies updated:
cd ../examples/approval
rm -rf target
bal clean
bal build