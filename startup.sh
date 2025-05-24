# Install or update needed software
apt-get update
apt-get install -yq git supervisor python3 python3-pip python3-distutils
# pip install --upgrade pip virtualenv

# Fetch source code
export HOME=/root
git clone https://github.com/michaelconan/personal-reporting-airflow.git /opt/app

# Install Cloud Ops Agent
# sudo bash /opt/app/gce/add-google-cloud-ops-agent-repo.sh --also-install

# Account to own server process
useradd -m -d /home/pythonapp pythonapp

# Python environment setup
python3 -m venv /opt/app/env
/bin/bash -c "source /opt/app/env/bin/activate"
/opt/app/env/bin/pip install -r /opt/app/requirements.txt -c /opt/app/constraints.txt

# Set ownership to newly created account
chown -R pythonapp:pythonapp /opt/app

# Put supervisor configuration in proper place
cp /opt/app/python-app.conf /etc/supervisor/conf.d/python-app.conf

# Start service via supervisorctl
supervisorctl reread
supervisorctl update