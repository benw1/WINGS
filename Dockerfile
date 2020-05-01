FROM stips:latest

# Make directory for WINGS
RUN mkdir /app \
    mkdir /app/WINGS

# I like emacs
RUN apt-get install -y emacs vim

# Lets copy over all the files
COPY . /app/WINGS

# This part doesn't correctly install too the right conda environment
# Run python setup
RUN conda install -c bioconda mysqlclient
RUN conda install -c anaconda mysql-connector-python
RUN pip install /app/WINGS

# expose the port we need
EXPOSE 3306
