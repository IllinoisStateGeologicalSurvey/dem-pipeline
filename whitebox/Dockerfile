FROM python:3.6

# install the notebook package
RUN pip install --no-cache --upgrade pip \
    && pip install --no-cache whitebox dask distributed \
    && python -c 'import whitebox'
COPY app /
