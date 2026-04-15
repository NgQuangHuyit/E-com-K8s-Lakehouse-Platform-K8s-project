FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN sed -i 's|http://archive.ubuntu.com|http://vn.archive.ubuntu.com|g' /etc/apt/sources.list && \
    sed -i 's|http://security.ubuntu.com|http://vn.archive.ubuntu.com|g' /etc/apt/sources.list

# Install SSH + Python
RUN apt-get update && \
    apt-get install -y openssh-server python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create SSH directories
RUN mkdir -p /var/run/sshd

# Create a user for SSH
ARG SSH_USER=dev
ARG SSH_PASS=devpass

RUN useradd -m -s /bin/bash $SSH_USER && \
    echo "$SSH_USER:$SSH_PASS" | chpasswd

# Allow password authentication
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Expose SSH port
EXPOSE 22

# Create example directory for logs
RUN mkdir -p /home/$SSH_USER/logs && chown $SSH_USER:$SSH_USER /home/$SSH_USER/logs

COPY ./generator.py /home/$SSH_USER/generator.py
COPY ./bulk_insert_customers.py /home/$SSH_USER/bulk_insert_customers.py
COPY ./requirements_db.txt /home/$SSH_USER/requirements_db.txt
COPY ./metadata /home/$SSH_USER/metadata
RUN chown $SSH_USER:$SSH_USER /home/$SSH_USER/generator.py
RUN chown $SSH_USER:$SSH_USER /home/$SSH_USER/bulk_insert_customers.py
RUN chown -R $SSH_USER:$SSH_USER /home/$SSH_USER/metadata
RUN chown $SSH_USER:$SSH_USER /home/$SSH_USER/requirements_db.txt

USER dev 

RUN pip install --no-cache-dir -r /home/$SSH_USER/requirements_db.txt

USER root
    # Start SSHD
CMD ["/usr/sbin/sshd", "-D"]
