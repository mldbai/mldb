FROM quay.io/datacratic/mldb_autobuild:MLDB-756-plugin-sdk
LABEL datacraticd.mldb_sdk=1
VOLUME /source2
COPY install_sdk.sh /source2/
RUN /source2/install_sdk.sh
