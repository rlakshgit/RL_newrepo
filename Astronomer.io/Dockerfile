FROM --platform=linux/amd64 quay.io/astronomer/astro-runtime:8.9.0-base


# The following is to support the bq command line tool, which is not included in the runtime image.
# This is required for the Archive_Dataset_or_Table_DAG to work.
USER root

COPY JMPCRT01-CA.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

COPY jmi-pip.conf /root/.config/pip/pip.conf

COPY packages.txt .
RUN if [[ -s packages.txt ]]; then apt-get update && cat packages.txt | tr '\r\n' '\n' | sed -e 's/#.*//' | xargs apt-get install -y --no-install-recommends     && apt-get clean     && rm -rf /var/lib/apt/lists/*;   fi

COPY requirements.txt .
RUN pip install --no-cache-dir -q -r requirements.txt 

#RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
#RUN curl --cacert  /etc/ssl/certs/JMPCRT01-CA.pem https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
#RUN sudo apt-get update && sudo apt-get -y install google-cloud-cli

USER astro
