cd native
mvn clean package

cd ../ballerina
bal build
bal pack
bal push --repository=local

# If example changed or dependencies updated:
cd ../examples/approval && rm -rf target && bal build