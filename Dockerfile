FROM amazon/aws-lambda-python:3.10

RUN /var/lang/bin/python3.10 -m pip install --upgrade pip

COPY requirements.txt
RUN pip install -r requirements.txt

COPY shopifyInventoryForecaster/ .

CMD ["lambda_function.lambda_handler"]