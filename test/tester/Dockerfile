FROM python

RUN mkdir -p /home/tester/tester
COPY . /home/tester/tester
WORKDIR /home/tester
RUN pip install -r tester/requirements.txt

CMD python3 -m tester
