#
# See README.md for notes on building this image
#

FROM quay.io/mldb/mldb_base:14.04
ENV DEBIAN_FRONTEND=noninteractive

ADD files/cuda-repo-ubuntu1404-8-0-local_8.0.44-1_amd64.deb \
    files/libcudnn5_5.1.5-1+cuda8.0_amd64.deb \
    files/libcudnn5-dev_5.1.5-1+cuda8.0_amd64.deb \
       /tmp/

RUN dpkg -i /tmp/cuda-repo-ubuntu1404-8-0-local_8.0.44-1_amd64.deb && \
      apt-get update && \
      apt-get install -y software-properties-common  python-software-properties && \
      curl http://saltmaster.mldb.ai/deb/ops_datacratic.pubkey | apt-key add - && \
      add-apt-repository -y "http://saltmaster.mldb.ai/deb/graphics-drivers-375.26 main" && \
      apt-get update && \
      apt-get install -y cuda-toolkit-8-0 && \
      apt-get install --no-install-recommends -y libcuda1-375 nvidia-375 nvidia-375-dev nvidia-opencl-icd-375 && \
      apt-get upgrade -y

RUN dpkg -i /tmp/libcudnn5_5.1.5-1+cuda8.0_amd64.deb && dpkg -i /tmp/libcudnn5-dev_5.1.5-1+cuda8.0_amd64.deb
RUN apt-get install -y python-pip python2.7-dev && \
        pip install --upgrade https://storage.googleapis.com/tensorflow/linux/gpu/tensorflow-0.11.0-cp27-none-linux_x86_64.whl

RUN apt-get remove -y  cuda-repo-ubuntu1404-8-0-local software-properties-common python-software-properties && \
        apt-get autoremove -y --purge && \
        rm -rf /var/lib/apt/lists/* && \
        rm -rf /root/.cache && \
        rm -rf /tmp/*deb || true

